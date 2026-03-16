import csv
import io
from typing import Any

from pydantic import BaseModel, ConfigDict

from anaplan_orm.parsers import DataParser


class AnaplanModel(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="ignore")

    @classmethod
    def from_payload(cls, payload: Any, parser: "DataParser", **kwargs) -> list["AnaplanModel"]:
        """
        Ingests a raw payload, parses it into dictionaries using the provided parser,
        and inflates them into validated Pydantic models.
        """
        # Dynamically build the extraction map by inspecting the Pydantic fields
        extraction_mapping = {}
        for field_name, field_info in cls.model_fields.items():
            # Check if the developer defined a custom extraction 'path' in json_schema_extra
            extra = field_info.json_schema_extra
            if extra and isinstance(extra, dict) and "path" in extra:
                # Map the Anaplan CSV column name (the alias) to the JMESPath
                target_key = field_info.alias if field_info.alias else field_name
                extraction_mapping[target_key] = extra["path"]

        # 2. Pass the paths to the parser via kwargs
        if extraction_mapping:
            kwargs["mapping"] = extraction_mapping

        # Parse the payload (the parser will use the mapping if it exists)
        parsed_data = parser.parse(payload, **kwargs)

        # Inflate the Pydantic objects
        return [cls(**row) for row in parsed_data]

    @classmethod
    def to_csv(cls, instances: list["AnaplanModel"], separator: str = ",") -> str:
        """Converts a list of AnaplanModel instances into a CSV formatted string."""
        if not instances:
            return ""

        # Safely extract headers using aliases
        headers = [field.alias if field.alias else name for name, field in cls.model_fields.items()]

        output = io.StringIO()
        # Force strict Unix line endings so Anaplan always parses correctly
        writer = csv.writer(output, delimiter=separator, lineterminator="\n")

        writer.writerow(headers)
        for instance in instances:
            writer.writerow(instance.model_dump(by_alias=True).values())

        return output.getvalue()
