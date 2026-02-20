"""Structure validation methods for LocalDevDriverDebugger."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlmodel import select

from kent.driver.persistent_driver.models import (
    Request,
)
from kent.driver.persistent_driver.sql_manager import (
    SQLManager,
)


class ValidationMixin:
    """Structure validation: XSD/JSON model validation of stored responses."""

    sql: SQLManager
    _session_factory: async_sessionmaker

    if TYPE_CHECKING:
        # Provided by DebuggerBase at runtime via multiple inheritance.
        async def get_run_metadata(
            self,
        ) -> dict[str, Any] | None: ...

    def _load_scraper_class(self, metadata: dict[str, Any]) -> type:
        """Load scraper class from run metadata.

        Args:
            metadata: Run metadata dict containing scraper_name.

        Returns:
            The scraper class.

        Raises:
            ValueError: If no scraper_name in metadata.
            ImportError: If scraper cannot be imported.
        """
        import importlib

        scraper_name = metadata.get("scraper_name")
        if not scraper_name:
            raise ValueError("No scraper_name in run metadata")

        if ":" in scraper_name:
            module_path, class_name = scraper_name.rsplit(":", 1)
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        else:
            module = importlib.import_module(scraper_name)
            return module.Site

    def _discover_steps_with_specs(
        self, scraper_class: type
    ) -> list[dict[str, Any]]:
        """Discover all steps that have xsd or json_model specs.

        Args:
            scraper_class: The scraper class to inspect.

        Returns:
            List of dicts with keys: name, spec_type, spec_value, metadata.
        """
        from kent.common.decorators import (
            get_step_metadata,
        )

        steps_with_specs = []
        for attr_name in dir(scraper_class):
            if attr_name.startswith("_"):
                continue
            try:
                method = getattr(scraper_class, attr_name)
                meta = get_step_metadata(method)
                if meta is None:
                    continue
                if meta.xsd:
                    steps_with_specs.append(
                        {
                            "name": attr_name,
                            "spec_type": "xsd",
                            "spec_value": meta.xsd,
                            "metadata": meta,
                        }
                    )
                elif meta.json_model:
                    steps_with_specs.append(
                        {
                            "name": attr_name,
                            "spec_type": "json_model",
                            "spec_value": meta.json_model,
                            "metadata": meta,
                        }
                    )
            except Exception:
                continue
        return steps_with_specs

    def _resolve_xsd_path(self, scraper_class: type, relative_xsd: str) -> str:
        """Resolve XSD path relative to scraper module directory.

        Args:
            scraper_class: The scraper class (to find module location).
            relative_xsd: Relative XSD path from step decorator.

        Returns:
            Absolute path to the XSD file.
        """
        import inspect

        module_file = inspect.getfile(scraper_class)
        module_dir = Path(module_file).parent
        return str(module_dir / relative_xsd)

    def _resolve_json_model(
        self, scraper_class: type, dotted_path: str
    ) -> type[Any]:
        """Resolve a dotted json_model path to a Pydantic model class.

        Args:
            scraper_class: The scraper class (to determine base package).
            dotted_path: Dotted path like "api.responses.ModelName".

        Returns:
            The Pydantic model class.
        """
        import importlib

        parts = dotted_path.rsplit(".", 1)
        if len(parts) == 1:
            raise ValueError(
                f"json_model must be dotted path like 'module.ClassName', got: {dotted_path}"
            )
        module_rel, class_name = parts

        scraper_module = scraper_class.__module__
        scraper_package = scraper_module.rsplit(".", 1)[0]
        full_module = f"{scraper_package}.{module_rel}"

        module = importlib.import_module(full_module)
        return getattr(module, class_name)

    async def validate_structure(
        self,
        step_name: str | None = None,
    ) -> dict[str, Any]:
        """Validate stored responses against step XSD/JSON model specs.

        Args:
            step_name: Optional filter to validate only a specific step.

        Returns:
            Dictionary with validation results:
                - steps: List of per-step results with counts and invalid IDs
                - summary: Aggregate counts across all steps
        """
        metadata = await self.get_run_metadata()
        if not metadata:
            raise ValueError("No run metadata found")

        scraper_class = self._load_scraper_class(metadata)
        specs = self._discover_steps_with_specs(scraper_class)

        if step_name:
            specs = [s for s in specs if s["name"] == step_name]

        step_results = []
        for spec in specs:
            continuation = spec["name"]
            spec_type = spec["spec_type"]
            spec_value = spec["spec_value"]

            async with self._session_factory() as session:
                count_result = await session.execute(
                    select(sa.func.count())
                    .select_from(Request)
                    .where(
                        Request.continuation == continuation,
                        Request.response_status_code.isnot(None),  # type: ignore[union-attr]
                    )
                )
                total = count_result.scalar() or 0

            if total == 0:
                step_results.append(
                    {
                        "continuation": continuation,
                        "spec_type": spec_type,
                        "spec_path": spec_value,
                        "total_responses": 0,
                        "valid": 0,
                        "invalid": 0,
                        "invalid_request_ids": [],
                        "invalid_response_ids": [],
                    }
                )
                continue

            if spec_type == "xsd":
                xsd_path = self._resolve_xsd_path(scraper_class, spec_value)
                invalid_request_ids = await self.sql.validate_xml_responses(
                    continuation, xsd_path
                )
            else:
                model_class = self._resolve_json_model(
                    scraper_class, spec_value
                )
                invalid_request_ids = await self.sql.validate_json_responses(
                    continuation, model_class
                )

            invalid_response_ids = list(invalid_request_ids)

            invalid_count = len(invalid_request_ids)
            step_results.append(
                {
                    "continuation": continuation,
                    "spec_type": spec_type,
                    "spec_path": spec_value,
                    "total_responses": total,
                    "valid": total - invalid_count,
                    "invalid": invalid_count,
                    "invalid_request_ids": invalid_request_ids,
                    "invalid_response_ids": invalid_response_ids,
                }
            )

        total_steps = len(step_results)
        total_responses = sum(s["total_responses"] for s in step_results)
        total_valid = sum(s["valid"] for s in step_results)
        total_invalid = sum(s["invalid"] for s in step_results)

        return {
            "steps": step_results,
            "summary": {
                "total_steps_with_specs": total_steps,
                "total_responses_checked": total_responses,
                "total_valid": total_valid,
                "total_invalid": total_invalid,
            },
        }

    async def validate_structure_detail(
        self,
        request_id: int | None = None,
        response_id: int | None = None,
    ) -> dict[str, Any]:
        """Get detailed validation output for a specific request or response.

        Args:
            request_id: Validate the response for this request ID.
            response_id: Validate this specific response ID.

        Returns:
            Dictionary with detailed validation results including error messages.

        Raises:
            ValueError: If neither request_id nor response_id is provided.
        """
        from lxml import etree
        from lxml import html as lxml_html
        from pydantic import ValidationError

        from kent.driver.persistent_driver.compression import (
            decompress_response,
        )

        if request_id is None and response_id is None:
            raise ValueError("Must provide either request_id or response_id")

        async with self._session_factory() as session:
            if response_id is not None:
                result = await session.execute(
                    select(
                        Request.id,
                        Request.continuation,
                        Request.content_compressed,
                        Request.compression_dict_id,
                    ).where(
                        Request.id == response_id,
                        Request.response_status_code.isnot(None),  # type: ignore[union-attr]
                    )
                )
                row = result.first()
                if not row:
                    raise ValueError(f"Response {response_id} not found")
                req_id, continuation, compressed, dict_id = row
                resp_id = req_id
            else:
                result = await session.execute(
                    select(
                        Request.id,
                        Request.continuation,
                        Request.content_compressed,
                        Request.compression_dict_id,
                    ).where(Request.id == request_id)
                )
                row = result.first()
                if not row:
                    raise ValueError(
                        f"No response found for request {request_id}"
                    )
                req_id, continuation, compressed, dict_id = row
                resp_id = req_id

        metadata = await self.get_run_metadata()
        if not metadata:
            raise ValueError("No run metadata found")

        scraper_class = self._load_scraper_class(metadata)
        specs = self._discover_steps_with_specs(scraper_class)
        spec = next((s for s in specs if s["name"] == continuation), None)

        if not spec:
            return {
                "request_id": req_id,
                "response_id": resp_id,
                "continuation": continuation,
                "spec_type": None,
                "status": "NO_SPEC",
                "message": f"Step '{continuation}' has no xsd or json_model spec",
            }

        if compressed is None:
            return {
                "request_id": req_id,
                "response_id": resp_id,
                "continuation": continuation,
                "spec_type": spec["spec_type"],
                "spec_path": spec["spec_value"],
                "status": "EMPTY",
                "message": "Response has no content",
            }

        content = await decompress_response(
            self._session_factory, compressed, dict_id
        )

        errors: list[str] = []
        if spec["spec_type"] == "xsd":
            xsd_path = self._resolve_xsd_path(
                scraper_class, spec["spec_value"]
            )
            try:
                schema_doc = etree.parse(xsd_path)  # noqa: S320
                schema = etree.XMLSchema(schema_doc)
                html_tree = lxml_html.fromstring(content)
                if not schema.validate(html_tree):
                    for log_entry in schema.error_log:
                        errors.append(str(log_entry))
            except Exception as e:
                errors.append(f"Validation error: {e}")
        else:
            model_class = self._resolve_json_model(
                scraper_class, spec["spec_value"]
            )
            try:
                content_str = content.decode("utf-8")
                data = json.loads(content_str)
                model_class.model_validate(data)  # type: ignore[attr-defined]
            except ValidationError as e:
                for err_dict in e.errors():
                    loc = " -> ".join(str(part) for part in err_dict["loc"])
                    errors.append(f"{loc}: {err_dict['msg']}")
            except Exception as e:
                errors.append(f"Validation error: {e}")

        status = "INVALID" if errors else "VALID"
        return {
            "request_id": req_id,
            "response_id": resp_id,
            "continuation": continuation,
            "spec_type": spec["spec_type"],
            "spec_path": spec["spec_value"],
            "status": status,
            "errors": errors,
        }
