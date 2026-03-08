import csv
import io
from pydantic import BaseModel, ConfigDict
from anaplan_orm.parsers import DataParser

class AnaplanModel(BaseModel):
    model_config = ConfigDict(
        str_strip_whitespace=True, 
        extra='ignore'             
    )

    @classmethod
    def from_payload(cls, payload: str, parser: DataParser) -> list['AnaplanModel']:
        """
        Ingests a raw payload using the injected parser, and converts
        the resulting dictionaries into validated Pydantic models.
        """
        raw_dicts = parser.parse(payload)
        validated_models = [cls(**row) for row in raw_dicts]
        return validated_models

    @classmethod
    def to_csv(cls, instances: list['AnaplanModel'], separator: str = ',') -> str:
        """
        Converts a list of AnaplanModel instances into a CSV formatted string.
        """
        if not instances:
            return ""
            
        # Extract headers directly from the Pydantic class schema
        headers = list(cls.model_fields.keys())
        
        # Set up the in-memory string buffer
        output = io.StringIO()
        writer = csv.writer(output, delimiter=separator)
        
        # Write headers and data
        writer.writerow(headers)
        for instance in instances:
            writer.writerow(instance.model_dump().values())
            
        return output.getvalue()