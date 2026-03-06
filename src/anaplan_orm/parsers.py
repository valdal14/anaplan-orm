import xml.etree.ElementTree as ET

class XMLStringParser():
    
    @classmethod
    def parse(self, xml_str_payload):
        if not isinstance(xml_str_payload, str):
            raise TypeError("Invalid Payload")
        else:
            payload = ET.fromstring(xml_str_payload)
            xml_elements = []
            if payload.tag == "":
                raise TypeError("No Root Element found")
            else:
                for row in payload:
                    xml_dic = {}
                    for child in row:
                       xml_dic[child.tag] = child.text
                    xml_elements.append(xml_dic)

                print(xml_elements)
                return xml_elements
