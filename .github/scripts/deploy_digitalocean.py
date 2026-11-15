"""Create/update the DigitalOcean resources used by Task Scheduler API."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit, urlunsplit
from urllib.request import Request, urlopen

API = "https://api.digitalocean.com/v2"
APP_NAME = "task-scheduler-api"
REGION = "sgp1"
APP_REGION = "sgp"
VPC_NAME = f"{APP_NAME}-vpc"
VPC_CIDR = "10.70.0.0/20"
DATABASE_NAME = f"{APP_NAME}-mongo"
RABBIT_NAME = f"{APP_NAME}-rabbitmq"
RABBIT_TAG = f"{APP_NAME}-rabbitmq"
RABBIT_USER = "taskapi"
REPOSITORY = "lijinstrades/task-scheduler-api"
TIMEOUT_SECONDS = 45 * 60


class DigitalOcean:
    def __init__(self, token: str) -> None:
        self.token = token

    def request(self, method: str, path: str, body: dict | None = None) -> dict:
        data = json.dumps(body).encode() if body is not None else None
        request = Request(
            f"{API}{path}",
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urlopen(request, timeout=60) as response:
                content = response.read()
                return json.loads(content) if content else {}
        except HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:500]
            secret = os.environ.get("RABBITMQ_PASSWORD", "")
            if secret:
                detail = detail.replace(secret, "[REDACTED]")
            raise RuntimeError(
                f"DigitalOcean API {method} {path} failed ({exc.code}): {detail}"
            ) from exc
        except URLError as exc:
            raise RuntimeError(f"DigitalOcean API {method} {path} failed: {exc.reason}") from exc

    def collection(self, path: str, key: str) -> list[dict]:
        result: list[dict] = []
        next_path: str | None = f"{path}?per_page=200"
        while next_path:
            response = self.request("GET", next_path)
            result.extend(response.get(key) or [])
            links = response.get("links") or {}
            pages = links.get("pages") or {}
            next_path = pages.get("next")
            if next_path and next_path.startswith(API):
                next_path = next_path[len(API) :]
        return result


def ensure_tag(do: DigitalOcean, name: str) -> None:
    if not any(tag["name"] == name for tag in do.collection("/tags", "tags")):
        do.request("POST", "/tags", {"name": name})


def wait_until(description: str, check, timeout: int = TIMEOUT_SECONDS):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        print(f"Waiting for {description}...")
        time.sleep(15)
    raise TimeoutError(f"Timed out waiting for {description}")


def ensure_vpc(do: DigitalOcean) -> dict:
    matches = [vpc for vpc in do.collection("/vpcs", "vpcs") if vpc["name"] == VPC_NAME]
    if matches:
        vpc = matches[0]
        if vpc["region"] != REGION or vpc["ip_range"] != VPC_CIDR:
            raise RuntimeError(f"Existing VPC {VPC_NAME} has an unexpected region or CIDR")
        return vpc
    return do.request(
        "POST",
        "/vpcs",
        {"name": VPC_NAME, "region": REGION, "ip_range": VPC_CIDR},
    )["vpc"]


def ensure_database(do: DigitalOcean, vpc_id: str) -> dict:
    matches = [
        cluster
        for cluster in do.collection("/databases", "databases")
        if cluster["name"] == DATABASE_NAME
    ]
    if matches:
        database = matches[0]
        if database["engine"] != "mongodb" or database["region"] != REGION:
            raise RuntimeError(f"Existing database {DATABASE_NAME} has an unexpected configuration")
        if database.get("private_network_uuid") != vpc_id:
            raise RuntimeError(f"Existing database {DATABASE_NAME} is not in the project VPC")
        return database
    return do.request(
        "POST",
        "/databases",
        {
            "name": DATABASE_NAME,
            "engine": "mongodb",
            "version": "8",
            "region": REGION,
            "size": "db-s-1vcpu-1gb",
            "num_nodes": 1,
            "private_network_uuid": vpc_id,
        },
    )["database"]


def ensure_database_firewall(do: DigitalOcean, database_id: str) -> None:
    response = do.request("GET", f"/databases/{database_id}/firewall")
    vpc_rule = {"type": "ip_addr", "value": VPC_CIDR, "description": f"{APP_NAME} VPC"}
    rules = response.get("rules", [])
    managed_rules = [
        {key: rule[key] for key in ("type", "value", "description") if key in rule}
        for rule in rules
    ]
    if managed_rules != [vpc_rule]:
        do.request("PUT", f"/databases/{database_id}/firewall", {"rules": [vpc_rule]})


def rabbit_user_data(password: str) -> str:
    # cloud-init's list form passes each value as an argument without shell expansion.
    cloud_config = {
        "package_update": True,
        "packages": ["rabbitmq-server", "ufw"],
        "runcmd": [
            ["rabbitmq-plugins", "enable", "--offline", "rabbitmq_management"],
            ["systemctl", "restart", "rabbitmq-server"],
            ["rabbitmqctl", "wait", "/var/run/rabbitmq/pid"],
            ["rabbitmqctl", "delete_user", "guest"],
            ["rabbitmqctl", "add_user", RABBIT_USER, password],
            ["rabbitmqctl", "set_user_tags", RABBIT_USER, "management"],
            ["rabbitmqctl", "set_permissions", "-p", "/", RABBIT_USER, ".*", ".*", ".*"],
            ["ufw", "default", "deny", "incoming"],
            ["ufw", "default", "allow", "outgoing"],
            ["ufw", "allow", "from", VPC_CIDR, "to", "any", "port", "5672", "proto", "tcp"],
            ["ufw", "allow", "from", VPC_CIDR, "to", "any", "port", "15672", "proto", "tcp"],
            ["ufw", "--force", "enable"],
        ],
    }
    return "#cloud-config\n" + json.dumps(cloud_config)


def ensure_rabbit_firewall(do: DigitalOcean) -> None:
    inbound = [
        {
            "protocol": "tcp",
            "ports": port,
            "sources": {"addresses": [VPC_CIDR]},
        }
        for port in ("5672", "15672")
    ]
    outbound = [
        {"protocol": "tcp", "ports": "1-65535", "destinations": {"addresses": ["0.0.0.0/0"]}},
        {"protocol": "udp", "ports": "1-65535", "destinations": {"addresses": ["0.0.0.0/0"]}},
        {"protocol": "icmp", "ports": "0", "destinations": {"addresses": ["0.0.0.0/0"]}},
    ]
    matches = [
        firewall
        for firewall in do.collection("/firewalls", "firewalls")
        if firewall["name"] == RABBIT_NAME
    ]
    body = {
        "name": RABBIT_NAME,
        "tags": [RABBIT_TAG],
        "inbound_rules": inbound,
        "outbound_rules": outbound,
    }
    if matches:
        do.request("PUT", f"/firewalls/{matches[0]['id']}", body)
    else:
        do.request("POST", "/firewalls", body)


def ensure_rabbit_droplet(do: DigitalOcean, vpc_id: str, password: str) -> dict:
    matches = [
        droplet
        for droplet in do.collection("/droplets", "droplets")
        if droplet["name"] == RABBIT_NAME
    ]
    if matches:
        droplet = matches[0]
        if droplet.get("vpc_uuid") != vpc_id:
            raise RuntimeError(f"Existing Droplet {RABBIT_NAME} is not in the project VPC")
        return droplet
    return do.request(
        "POST",
        "/droplets",
        {
            "name": RABBIT_NAME,
            "region": REGION,
            "size": "s-1vcpu-2gb",
            "image": "ubuntu-24-04-x64",
            "vpc_uuid": vpc_id,
            "tags": [APP_NAME, RABBIT_TAG],
            "user_data": rabbit_user_data(password),
            "monitoring": True,
        },
    )["droplet"]


def rabbit_private_ip(do: DigitalOcean, droplet: dict) -> str:
    networks = droplet.get("networks", {}).get("v4", [])
    private = [network["ip_address"] for network in networks if network.get("type") == "private"]
    if private:
        return private[0]
    refreshed = do.request("GET", f"/droplets/{droplet['id']}")["droplet"]
    networks = refreshed.get("networks", {}).get("v4", [])
    private = [network["ip_address"] for network in networks if network.get("type") == "private"]
    if not private:
        raise RuntimeError("RabbitMQ Droplet has no private VPC address")
    return private[0]


def database_uri(do: DigitalOcean, database_id: str) -> str:
    """Build the private Mongo URI from the cluster's API connection details."""
    cluster = do.request("GET", f"/databases/{database_id}").get("database", {})
    connection = cluster.get("private_connection") or {}
    username = connection.get("user")
    password = os.environ.get("MONGODB_PASSWORD") or connection.get("password")
    if username and not password:
        user = do.request("GET", f"/databases/{database_id}/users/{quote(username, safe='')}").get(
            "user", {}
        )
        password = user.get("password")

    missing = [
        field
        for field, value in (
            ("host", connection.get("host")),
            ("user", username),
            ("password", password),
        )
        if not value
    ]
    if missing:
        detail = ", ".join(missing)
        if "password" in missing:
            detail += (
                "; the cluster and database-user API responses both omitted the password. "
                "Confirm the DIGITALOCEAN_ACCESS_TOKEN GitHub secret contains "
                "the full-scoped token "
                "with database:view_credentials permission"
            )
        raise RuntimeError(
            f"DigitalOcean did not return required private MongoDB connection field(s): {detail}"
        )

    uri_parts = urlsplit(connection.get("uri", ""))
    host = connection["host"]
    query = uri_parts.query or ("tls=true" if connection.get("ssl") else "")
    return urlunsplit(
        (
            "mongodb+srv",
            f"{quote(username, safe='')}:{quote(password, safe='')}@{host}",
            "/job_api",
            query,
            "",
        )
    )


def app_spec(vpc_id: str, mongo_uri: str, rabbit_ip: str, rabbit_password: str) -> dict:
    rabbit_user = quote(RABBIT_USER, safe="")
    rabbit_secret = quote(rabbit_password, safe="")
    common_envs = [
        {"key": "MONGO_URI", "value": mongo_uri, "scope": "RUN_TIME", "type": "SECRET"},
        {"key": "MONGO_DATABASE", "value": "job_api", "scope": "RUN_TIME", "type": "GENERAL"},
        {
            "key": "RABBITMQ_URL",
            "value": f"amqp://{rabbit_user}:{rabbit_secret}@{rabbit_ip}:5672/%2F",
            "scope": "RUN_TIME",
            "type": "SECRET",
        },
        {
            "key": "RABBITMQ_MANAGEMENT_URL",
            "value": f"http://{rabbit_ip}:15672",
            "scope": "RUN_TIME",
            "type": "GENERAL",
        },
        {
            "key": "RABBITMQ_MANAGEMENT_USER",
            "value": RABBIT_USER,
            "scope": "RUN_TIME",
            "type": "GENERAL",
        },
        {
            "key": "RABBITMQ_MANAGEMENT_PASSWORD",
            "value": rabbit_password,
            "scope": "RUN_TIME",
            "type": "SECRET",
        },
        {"key": "WORKER_COUNT", "value": "1", "scope": "RUN_TIME", "type": "GENERAL"},
        {
            "key": "ENABLE_TEST_HANDLERS",
            "value": "false",
            "scope": "RUN_TIME",
            "type": "GENERAL",
        },
    ]
    source = {
        "git": {
            "repo_clone_url": f"https://github.com/{REPOSITORY}.git",
            "branch": "main",
        },
        "environment_slug": "python",
        "build_command": "python -m pip install .",
    }
    return {
        "name": APP_NAME,
        "region": APP_REGION,
        "vpc": {"id": vpc_id},
        "services": [
            {
                **source,
                "name": "api",
                "run_command": "python -m uvicorn job_api.api:app --host 0.0.0.0 --port 8080",
                "http_port": 8080,
                "instance_size_slug": "apps-s-1vcpu-1gb",
                "instance_count": int(os.getenv("DO_API_INSTANCES", "1")),
                "health_check": {"http_path": "/health", "initial_delay_seconds": 60},
                "envs": common_envs,
            }
        ],
        "workers": [
            {
                **source,
                "name": "worker",
                "run_command": "python -m job_api.pool",
                "instance_size_slug": "apps-s-1vcpu-1gb",
                "instance_count": int(os.getenv("DO_WORKER_INSTANCES", "1")),
                "envs": common_envs,
            }
        ],
        "ingress": {
            "rules": [
                {
                    "match": {"path": {"prefix": "/"}},
                    "component": {"name": "api"},
                }
            ]
        },
    }


def redact_app_secrets(message: str, spec: dict) -> str:
    for component in [*spec.get("services", []), *spec.get("workers", [])]:
        for env in component.get("envs", []):
            value = env.get("value")
            if env.get("type") == "SECRET" and value:
                message = message.replace(value, "[REDACTED]")
    return message


def failed_api_logs(details: str, spec: dict) -> str:
    app_match = re.search(r"app \(([0-9a-f-]{36})\)", details)
    deployment_match = re.search(r"deployment ID: ([0-9a-f-]{36})", details)
    if not app_match or not deployment_match:
        return ""
    app_id, deployment_id = app_match.group(1), deployment_match.group(1)
    attempts = [
        ["--type", "run_restarted", "--tail", "100"],
        ["--type", "run", "--deployment", deployment_id, "--tail", "100"],
        ["--type", "deploy", "--deployment", deployment_id, "--tail", "100"],
    ]
    outputs = []
    for arguments in attempts:
        logs = subprocess.run(
            ["doctl", "apps", "logs", app_id, "api", *arguments],
            check=False,
            capture_output=True,
            text=True,
        )
        output = logs.stdout.strip() or logs.stderr.strip()
        if output:
            outputs.append(f"$ doctl apps logs api {' '.join(arguments)}\n{output}")
    return redact_app_secrets("\n\n".join(outputs), spec)


def main() -> None:
    token = os.environ.get("DIGITALOCEAN_ACCESS_TOKEN", "")
    rabbit_password = os.environ.get("RABBITMQ_PASSWORD", "")
    if not token or len(rabbit_password) < 24:
        raise RuntimeError(
            "Set DIGITALOCEAN_ACCESS_TOKEN and a RABBITMQ_PASSWORD of at least 24 characters"
        )
    for variable in ("DO_API_INSTANCES", "DO_WORKER_INSTANCES"):
        if not os.getenv(variable, "1").isdigit() or int(os.getenv(variable, "1")) < 1:
            raise RuntimeError(f"{variable} must be a positive integer")

    do = DigitalOcean(token)
    vpc = ensure_vpc(do)
    ensure_tag(do, APP_NAME)
    ensure_tag(do, RABBIT_TAG)
    database = ensure_database(do, vpc["id"])
    ensure_rabbit_firewall(do)
    droplet = ensure_rabbit_droplet(do, vpc["id"], rabbit_password)

    def active_droplet():
        current = do.request("GET", f"/droplets/{droplet['id']}")["droplet"]
        return current if current["status"] == "active" else None

    def ready_database():
        current = do.request("GET", f"/databases/{database['id']}")["database"]
        return current if current.get("status") == "online" else None

    droplet = wait_until("RabbitMQ Droplet activation", active_droplet)
    database = wait_until("MongoDB cluster readiness", ready_database)
    ensure_database_firewall(do, database["id"])
    private_ip = rabbit_private_ip(do, droplet)
    mongo_uri = database_uri(do, database["id"])
    spec = app_spec(vpc["id"], mongo_uri, private_ip, rabbit_password)
    with tempfile.TemporaryDirectory() as temporary_directory:
        spec_path = Path(temporary_directory) / "app-spec.json"
        spec_path.write_text(json.dumps(spec), encoding="utf-8")
        os.chmod(spec_path, 0o600)
        result = subprocess.run(
            [
                "doctl",
                "apps",
                "create",
                "--upsert",
                "--spec",
                str(spec_path),
                "--update-sources",
                "--wait",
                "--wait-timeout",
                "45m",
                "--format",
                "ID,DefaultIngress",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    if result.returncode:
        details = result.stderr.strip() or result.stdout.strip() or "no error details returned"
        api_logs = failed_api_logs(details, spec)
        if api_logs:
            details += "\nAPI runtime logs:\n" + api_logs
        raise RuntimeError("doctl app create/update failed: " + redact_app_secrets(details, spec))
    print(result.stdout.strip())
    print("DigitalOcean resources are ready and the App Platform deployment completed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Deployment failed: {error}", file=sys.stderr)
        raise
