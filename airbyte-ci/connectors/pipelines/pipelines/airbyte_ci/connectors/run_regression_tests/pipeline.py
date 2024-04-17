#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
"""This module groups factory like functions to dispatch tests steps according to the connector under test language."""

from __future__ import annotations

import anyio
from connector_ops.utils import ConnectorLanguage  # type: ignore
from pipelines.airbyte_ci.connectors.context import ConnectorContext
from pipelines.airbyte_ci.connectors.run_regression_tests.steps import get_test_steps
from pipelines.airbyte_ci.connectors.reports import ConnectorReport
from pipelines.helpers.execution.run_steps import run_steps


async def run_regression_tests_pipeline(context: ConnectorContext, semaphore: anyio.Semaphore):
    async with semaphore:
        async with context:
            result_dict = await run_steps(
                runnables=get_test_steps(context),
                options=context.run_step_options,
            )

            results = list(result_dict.values())
            report = ConnectorReport(context, steps_results=results, name="TEST RESULTS")
            context.report = report

        return report
