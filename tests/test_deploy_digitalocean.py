import importlib.util
from pathlib import Path
from unittest.mock import Mock, call

SCRIPT = Path(__file__).resolve().parents[1] / ".github/scripts/deploy_digitalocean.py"
SPEC = importlib.util.spec_from_file_location("deploy_digitalocean", SCRIPT)
deploy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(deploy)


def test_app_id_finds_task_scheduler_app():
    do = Mock()
    do.collection.return_value = [
        {"id": "other-id", "spec": {"name": "other-app"}},
        {"id": "task-app-id", "spec": {"name": deploy.APP_NAME}},
    ]

    assert deploy.app_id(do) == "task-app-id"
    do.collection.assert_called_once_with("/apps", "apps")


def test_database_firewall_trusts_vpc_and_app():
    do = Mock()
    do.request.side_effect = [{"rules": []}, {}]

    deploy.ensure_database_firewall(do, "mongo-id", "task-app-id")

    expected_rules = [
        {"type": "ip_addr", "value": deploy.VPC_CIDR, "description": f"{deploy.APP_NAME} VPC"},
        {
            "type": "app",
            "value": "task-app-id",
            "description": f"{deploy.APP_NAME} App Platform app",
        },
    ]
    assert do.request.call_args_list == [
        call("GET", "/databases/mongo-id/firewall"),
        call("PUT", "/databases/mongo-id/firewall", {"rules": expected_rules}),
    ]


def test_database_firewall_does_not_update_when_rules_already_match():
    do = Mock()
    expected_rules = [
        {"type": "ip_addr", "value": deploy.VPC_CIDR, "description": f"{deploy.APP_NAME} VPC"},
        {
            "type": "app",
            "value": "task-app-id",
            "description": f"{deploy.APP_NAME} App Platform app",
        },
    ]
    do.request.return_value = {"rules": expected_rules}

    deploy.ensure_database_firewall(do, "mongo-id", "task-app-id")

    do.request.assert_called_once_with("GET", "/databases/mongo-id/firewall")
