#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

"""This module groups steps made to run regression tests for any Source connector."""

from pipelines.airbyte_ci.connectors.build_image.steps.python_connectors import BuildConnectorImages
from pipelines.airbyte_ci.connectors.consts import CONNECTOR_TEST_STEP_ID
from pipelines.airbyte_ci.connectors.context import ConnectorContext
from pipelines.airbyte_ci.connectors.test.steps.common import RegressionTests
from pipelines.consts import LOCAL_BUILD_PLATFORM
from pipelines.helpers.execution.run_steps import STEP_TREE, StepToRun

_BASE_CONTAINER_DIRECTORY = "/tmp"
_CONTAINER_TEST_OUTPUT_DIRECTORY = f"{_BASE_CONTAINER_DIRECTORY}/test_output"
_CONTAINER_EXPECTED_RECORDS_DIRECTORY = f"{_CONTAINER_TEST_OUTPUT_DIRECTORY}/expected_records"
_CONTAINER_ACCEPTANCE_TEST_CONFIG_FILEPATH = f"{_BASE_CONTAINER_DIRECTORY}/updated-acceptance-test-config.yml"
_HOST_TEST_OUTPUT_DIRECTORY = "/tmp/test_dir"
_REGRESSION_TEST_DIRECTORY = "/app/connector_acceptance_test/utils/regression_test.py"


class BuildConnectorImagesControl(BuildConnectorImages):
    @property
    def title(self):
        return f"{super().title}: Control Container ({self.docker_image_name})"


class BuildConnectorImagesTarget(BuildConnectorImages):
    @property
    def title(self):
        return f"{super().title}: Target Container ({self.docker_image_name})"


def get_test_steps(context: ConnectorContext) -> STEP_TREE:
    """
    Get all the tests steps for running regression tests.
    """
    control_version, target_version = context.regression_test_versions
    control_image = f"airbyte/{context.connector.technical_name}:{control_version}"
    target_image = f"airbyte/{context.connector.technical_name}:{target_version}"

    return [
        [
            StepToRun(
                id=CONNECTOR_TEST_STEP_ID.REGRESSION_TEST_BUILD_CONTROL,
                step=BuildConnectorImagesControl(context, docker_image_name=control_image),
            ),
        ],
        [
            StepToRun(
                id=CONNECTOR_TEST_STEP_ID.REGRESSION_TEST_BUILD_TARGET,
                step=BuildConnectorImagesTarget(context, docker_image_name=None if target_version == "dev" else target_image),
            )
        ],
        [
            StepToRun(
                id=CONNECTOR_TEST_STEP_ID.REGRESSION_TESTS,
                step=RegressionTests(context),
                args=lambda results: {
                    "control_container": results[CONNECTOR_TEST_STEP_ID.REGRESSION_TEST_BUILD_CONTROL].output[
                        LOCAL_BUILD_PLATFORM
                    ],
                    "target_container": results[CONNECTOR_TEST_STEP_ID.REGRESSION_TEST_BUILD_TARGET].output[
                        LOCAL_BUILD_PLATFORM
                    ]
                },
                depends_on=[CONNECTOR_TEST_STEP_ID.REGRESSION_TEST_BUILD_CONTROL, CONNECTOR_TEST_STEP_ID.REGRESSION_TEST_BUILD_TARGET],
            )
        ],
    ]
