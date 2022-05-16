#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from typing import Any, List, Mapping, Optional

import requests
from airbyte_cdk.sources.lcc.interpolation.interpolated_mapping import InterpolatedMapping
from airbyte_cdk.sources.lcc.interpolation.jinja import JinjaInterpolation
from airbyte_cdk.sources.lcc.requesters.paginators.paginator import Paginator


class InterpolatedPaginator(Paginator):
    def __init__(self, next_page_token_template: Mapping[str, str], config):
        self._next_page_token_template = InterpolatedMapping(next_page_token_template, JinjaInterpolation())
        self._config = config

    def next_page_token(self, response: requests.Response, last_records: List[Mapping[str, Any]]) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        headers = response.headers
        # Pass in values as kwargs
        kwargs = {"decoded_response": decoded_response, "headers": headers, "last_records": last_records}
        interpolated_values = self._next_page_token_template.eval(self._config, **kwargs)

        return interpolated_values if interpolated_values else None
