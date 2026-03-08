import csv
import io
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
</AnaplanExport>
"""

# 1. Parse the data
xml_parser = XMLStringParser()
roster = EmployeeRoster.from_payload(payload=incoming_data, parser=xml_parser)

first_row = roster[0]
print(first_row)

