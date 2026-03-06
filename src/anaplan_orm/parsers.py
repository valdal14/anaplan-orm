import xml.etree.ElementTree as ET

class XMLStringParser:
    
    @classmethod
    def parse(cls, xml_str_payload: str) -> list[dict]:
        """Parses an XML string payload into a list of dictionaries."""
        
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