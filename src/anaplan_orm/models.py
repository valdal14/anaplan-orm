from pydantic import BaseModel, ConfigDict
from anaplan_orm.parsers import DataParser

class AnaplanModel(BaseModel):
    """
    The foundational class for the Anaplan ORM.
    Users will inherit from this to define their Anaplan targets.
    """
    
    # Pydantic V2 configuration
    model_config = ConfigDict(
        str_strip_whitespace=True,
        # Ignores unknown fields in the payload
        extra='ignore'              
    )

    @classmethod
    def from_payload(cls, payload: str, parser: DataParser) -> list['AnaplanModel']:
        """
        Ingests a raw payload using the injected parser, and converts
        the resulting dictionaries into validated Pydantic models.
        """
        # 1. Use the injected parser to get the raw list of dictionaries
        raw_dicts = parser.parse(payload)
        
        # 2. Convert those dictionaries into instances of this Pydantic class
        # (cls(**row) unpacks the dictionary into the Pydantic model)
        validated_models = [cls(**row) for row in raw_dicts]
        
        return validated_models