from abc import ABC, abstractmethod
import xml.etree.ElementTree as ET

class DataParser(ABC):
    """
    The abstract interface that all Anaplan ORM parsers must implement.
    
    This ensures that any custom parser injected into the AnaplanModel 
    adheres to a strict contract for data extraction.
    """
    
    @abstractmethod
    def parse(self, payload: str) -> list[dict]:
        """
        Parses a raw string payload into a list of dictionaries.

        Args:
            payload (str): The raw data string (e.g., XML, JSON) to be parsed.

        Returns:
            list[dict]: A list of flat dictionaries representing the extracted rows.
        """
        pass

class XMLStringParser(DataParser):
    """
    A concrete implementation of DataParser designed to handle XML data
    embedded within a standard string.
    """

    @classmethod
    def parse(cls, xml_str_payload: str) -> list[dict]:
        """
        Extracts row data from a flat XML string.

        Args:
            xml_str_payload (str): The raw XML string. Must contain a root element
                and child nodes representing rows of data.

        Raises:
            TypeError: If the payload is not a string or lacks a root element.

        Returns:
            list[dict]: A list where each dictionary is a row, with XML tags as keys
                and XML text as values.
        """
        
        if not isinstance(xml_str_payload, str):
            raise TypeError("Invalid Payload: Expected a string.")
            
        payload = ET.fromstring(xml_str_payload)
        xml_elements = []
        
        if payload.tag == "":
            raise TypeError("No Root Element found")
            
        for row in payload:
            xml_dic = {}
            for child in row:
                xml_dic[child.tag] = child.text
            xml_elements.append(xml_dic)

        return xml_elements