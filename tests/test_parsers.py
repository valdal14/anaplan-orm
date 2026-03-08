from anaplan_orm.models import AnaplanModel
from anaplan_orm.parsers import XMLStringParser

class EmployeeRoster(AnaplanModel):
    EmployeeID: int
    Department: str
    Salary: float

incoming_data = """
<AnaplanExport>
    <Row>
        <EmployeeID>101</EmployeeID>
        <Department>Sales</Department>
        <Salary>75000</Salary>
    </Row>
    <Row>
        <EmployeeID>102</EmployeeID>
        <Department>Engineering</Department>
        <Salary>90000</Salary>
    </Row>
</AnaplanExport>
"""

def test_xml_string_parser():
    # Arrange: Create an instance of the XMLStringParser
    xml_parser = XMLStringParser()
    # Act: From XML String create a list of AnaplanModel
    roster = EmployeeRoster.from_payload(payload=incoming_data, parser=xml_parser)
    # Assert: Verify the output is a list and it holds 2 elements
    assert type(roster) == list
    assert len(roster) == 2

def test_to_csv_serialization():
    # Arrange: Parse the data
    xml_parser = XMLStringParser()
    roster = EmployeeRoster.from_payload(payload=incoming_data, parser=xml_parser)
    
    # Act: Serialization method
    csv_output = EmployeeRoster.to_csv(roster)
    
    # Assert: Verify the output is a string and contains the right CSV data
    assert isinstance(csv_output, str)
    
    # Check that the headers were dynamically extracted and formatted
    assert "EmployeeID,Department,Salary" in csv_output
    
    # Check that the floats and strings were properly serialized
    assert "101,Sales,75000.0" in csv_output
    assert "102,Engineering,90000.0" in csv_output

def test_to_csv_custom_separator():
    # Arrange
    xml_parser = XMLStringParser()
    roster = EmployeeRoster.from_payload(payload=incoming_data, parser=xml_parser)
    
    # Act: Pass a pipe character '|' as the separator
    csv_output = EmployeeRoster.to_csv(roster, separator='|')
    
    # Assert
    assert isinstance(csv_output, str)
    assert "EmployeeID|Department|Salary" in csv_output
    assert "101|Sales|75000.0" in csv_output