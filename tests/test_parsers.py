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
    xml_parser = XMLStringParser()
    roster = EmployeeRoster.from_payload(payload=incoming_data, parser=xml_parser)
    assert type(roster) == list
    assert len(roster) == 2