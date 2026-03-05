from pydantic import BaseModel, ConfigDict

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
    def from_payload(cls, payload: str):
        """
        Future entry point to parse incoming data.
        """
        pass