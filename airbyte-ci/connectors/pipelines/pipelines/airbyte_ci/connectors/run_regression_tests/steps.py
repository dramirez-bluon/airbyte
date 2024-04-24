#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

"""This module groups steps made to run regression tests for any Source connector."""

import time
from pathlib import Path
from typing import List

import requests  # type: ignore
import yaml  # type: ignore
from dagger import Container, Directory, File
from pipelines import hacks
from pipelines.airbyte_ci.connectors.build_image.steps.python_connectors import BuildConnectorImages
from pipelines.airbyte_ci.connectors.consts import CONNECTOR_TEST_STEP_ID
from pipelines.airbyte_ci.connectors.context import ConnectorContext
from pipelines.consts import LOCAL_BUILD_PLATFORM
from pipelines.helpers.execution.run_steps import STEP_TREE, StepToRun
from pipelines.helpers.utils import get_exec_result
from pipelines.models.steps import STEP_PARAMS, Step, StepResult


class RegressionTests(Step):
    """A step to run regression tests for a connector."""

    context: ConnectorContext
    title = "Regression tests"
    skipped_exit_code = 5
    accept_extra_params = True
    regression_tests_artifacts_dir = Path("/tmp/regression_tests_artifacts")

    @property
    def default_params(self) -> STEP_PARAMS:
        """Default pytest options.

        Returns:
            dict: The default pytest options.
        """
        return super().default_params | {
            "-ra": [],  # Show extra test summary info in the report for all but the passed tests
            "--disable-warnings": [],  # Disable warnings in the pytest report
            "--durations": ["3"],  # Show the 3 slowest tests in the report
        }

    def regression_tests_command(self, start_timestamp: int) -> List[str]:
        return [
            "poetry",
            "run",
            "pytest",
            "src/live_tests/regression_tests",
            "--connector-image",
            self.connector_image,
            "--connection-id",
            self.connection_id,
            "--control-version",
            self.control_version,
            "--target-version",
            self.target_version,
            "--pr-url",
            self.pr_url,
            "--start-timestamp",
            str(start_timestamp),
            "--should-read-with-state",
            str(self.should_read_with_state),
        ]

    def __init__(self, context: ConnectorContext) -> None:
        """Create a step to run regression tests for a connector.

        Args:
            context (ConnectorContext): The current test context, providing a connector object, a dagger client and a repository directory.
        """
        super().__init__(context)
        self.connector_image = context.docker_image.split(":")[0]
        self.connection_id = context.regression_test_connection_id
        self.control_version, self.target_version = context.regression_test_versions
        self.pr_url = context.regression_test_pr_url
        self.should_read_with_state = context.should_read_with_state

    async def _run(self, connector_under_test: Container) -> StepResult:
        """Run the regression test suite.

        Args:
            connector_under_test (Container): The container holding the target connector test image.

        Returns:
            StepResult: Failure or success of the regression tests with stdout and stderr.
        """
        live_tests_dir = self.context.live_tests_dir
        start_timestamp = int(time.time())
        container = await self._build_regression_test_container(live_tests_dir, await connector_under_test.id())
        container = container.with_(hacks.never_fail_exec(self.regression_tests_command(start_timestamp)))
        regression_tests_artifacts_dir = str(self.regression_tests_artifacts_dir)
        await container.directory(regression_tests_artifacts_dir).export(regression_tests_artifacts_dir)
        path_to_report = f"{regression_tests_artifacts_dir}/session_{int(start_timestamp)}/report.html"
        exit_code, stdout, stderr = await get_exec_result(container)

        with open(path_to_report, "r") as fp:
            regression_test_report = fp.read()

        return StepResult(
            step=self,
            status=self.get_step_status_from_exit_code(exit_code),
            stderr=stderr,
            stdout=stdout,
            output=container,
            report=regression_test_report,
        )

    async def _build_regression_test_container(self, test_dir: Directory, target_container_id: str) -> Container:
        """Create a container to run regression tests."""
        image = "python:3.10-slim"

        container = self.dagger_client.container().from_(image)
        container = (container.with_exec([
            "apt-get", "update"
        ]).with_exec([
            "apt-get", "install", "-y", "git", "openssh-client", "curl", "docker.io"
        ]).with_exec([
            "bash", "-c", "curl https://sdk.cloud.google.com | bash"
        ]).with_env_variable(
         "PATH", "/root/google-cloud-sdk/bin:$PATH", expand=True
        ).with_mounted_file(
            "/root/.ssh/id_rsa", self.dagger_client.host().file(str(Path("~/.ssh/id_rsa").expanduser()))  # TODO
        ).with_mounted_file(
            "/root/.ssh/known_hosts",
            self.dagger_client.host().file(str(Path("~/.ssh/known_hosts").expanduser()))  # TODO
        ).with_mounted_file(
            "/root/.config/gcloud/application_default_credentials.json",
            self.dagger_client.host().file(str(Path("~/.config/gcloud/application_default_credentials.json").expanduser()))  # TODO
        ).with_mounted_directory(
            "/app", test_dir
        ).with_workdir(
            f"/app"
        ).with_exec([
            "pip", "install", "poetry"
        ]).with_exec(
            ["poetry", "lock", "--no-update"]
        ).with_exec([
            "poetry", "install"
        ])
        ).with_unix_socket(
            "/var/run/docker.sock", self.dagger_client.host().unix_socket("/var/run/docker.sock")
        ).with_env_variable(
            "IS_AIRBYTE_CI", "true"
        ).with_new_file(
            "/tmp/container_id.txt", contents=str(target_container_id)
        )
        return container


def get_test_steps(context: ConnectorContext) -> STEP_TREE:
    """
    Get all the tests steps for running regression tests.
    """
    return [
        [
            StepToRun(
                id=CONNECTOR_TEST_STEP_ID.BUILD,
                step=BuildConnectorImages(context),
            )
        ],
        [
            StepToRun(
                id=CONNECTOR_TEST_STEP_ID.REGRESSION_TESTS,
                step=RegressionTests(context),
                args=lambda results: {
                    "connector_under_test": results[CONNECTOR_TEST_STEP_ID.BUILD].output[
                        LOCAL_BUILD_PLATFORM
                    ]
                },
                depends_on=[CONNECTOR_TEST_STEP_ID.BUILD],
            )
        ],
    ]
