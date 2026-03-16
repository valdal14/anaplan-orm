import csv
import io
import json
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from typing import Any


class DataParser(ABC):
    """
    The abstract interface that all Anaplan ORM parsers must implement.

    This ensures that any custom parser injected into the AnaplanModel
    adheres to a strict contract for data extraction.
    """

    @abstractmethod
    def parse(self, payload: Any, **kwargs) -> list[dict]:
        """
        Parses a raw payload into a list of dictionaries.

        Args:
            payload (Any): The raw data (e.g., String, DB Cursor) to be parsed.

        Returns:
            list[dict]: A list of flat dictionaries representing the extracted rows.
        """
        pass


# ⬇️ OUTBOUND PIPELINE PARSERS (Anaplan -> Target) ########################################################
class CSVStringParser(DataParser):
    """
    A concrete implementation of DataParser designed to handle CSV data.

    PRIMARY USE CASE: Outbound Pipelines (Anaplan ➔ External Target).
    Because Anaplan strictly exports data as CSV payloads, this parser is used to take
    the raw string downloaded from the AnaplanClient and flat-map it into dictionaries
    so it can be inflated back into Pydantic ORM models for downstream transformation.
    """

    @classmethod
    def parse(cls, csv_str_payload: str, **kwargs) -> list[dict]:
        """
        Extracts row data from a flat CSV string.

        Args:
            csv_str_payload (str): The raw CSV string downloaded from Anaplan.

        Raises:
            TypeError: If the payload is not a string.
            ValueError: If the CSV string is empty or entirely whitespace.

        Returns:
            list[dict]: A list where each dictionary is a row, with CSV headers as keys
                and column data as values.
        """
        if not isinstance(csv_str_payload, str):
            raise TypeError("Invalid Payload: Expected a string.")

        if not csv_str_payload or not csv_str_payload.strip():
            raise ValueError("Cannot parse an empty CSV string.")

        # Use io.StringIO to turn the raw string into an in-memory file buffer
        string_buffer = io.StringIO(csv_str_payload.strip())

        # Use csv.DictReader to automatically read the first row as headers
        # and map all subsequent rows to those header keys.
        reader = csv.DictReader(string_buffer)

        # Convert the reader generator into a clean list of dictionaries
        csv_elements = [row for row in reader]

        return csv_elements


# ⬆️ INBOUND PIPELINE PARSERS (Source -> Anaplan) #########################################################
class XMLStringParser(DataParser):
    """
    A concrete implementation of DataParser designed to handle XML data
    embedded within a standard string.

    PRIMARY USE CASE: Inbound Pipelines (XML Source ➔ Anaplan).
    """

    @classmethod
    def parse(cls, xml_str_payload: str, **kwargs) -> list[dict]:
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


class JSONParser(DataParser):
    """
    A concrete implementation of DataParser designed to handle JSON data
    embedded within a standard string.

    PRIMARY USE CASE: Inbound Pipelines (REST API ➔ Anaplan).
    """

    @classmethod
    def parse(cls, json_str_payload: str, data_key: str = None, **kwargs) -> list[dict]:
        """
        Extracts row data from a JSON formatted string.

        Args:
            json_str_payload (str): The raw JSON string.
            data_key (str, optional): The key to extract if the array of records
                                      is nested inside a root JSON object.

        Raises:
            TypeError: If the payload is not a string.
            ValueError: If the payload is empty or invalid JSON.

        Returns:
            list[dict]: A list where each dictionary is a row of data.
        """
        if not isinstance(json_str_payload, str):
            raise TypeError("Invalid Payload: Expected a string.")

        if not json_str_payload or not json_str_payload.strip():
            raise ValueError("Cannot parse an empty JSON string.")

        # Load the JSON string
        try:
            parsed_data = json.loads(json_str_payload)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to decode JSON payload: {str(e)}")

        # Extract nested data if a data_key is requested
        if data_key is not None:
            if isinstance(parsed_data, dict):
                # Use .get() safely, defaulting to an empty list if the key doesn't exist
                parsed_data = parsed_data.get(data_key, [])
            else:
                raise ValueError(
                    f"Cannot extract data_key '{data_key}' because the JSON root is a list, not a dictionary."
                )

        # Ensure the return type is strictly a list[dict]
        if isinstance(parsed_data, dict):
            return [parsed_data]
        elif isinstance(parsed_data, list):
            return parsed_data
        else:
            raise TypeError("Parsed JSON must result in a dictionary or a list of dictionaries.")


class SQLCursorParser(DataParser):
    """
    A concrete implementation of DataParser designed to handle live database cursors.
    
    PRIMARY USE CASE: Inbound Pipelines (SQL Database ➔ Anaplan).
    """
    
    @classmethod
    def parse(cls, payload: Any, **kwargs) -> list[dict]:
        """
        Extracts row data from an active database cursor.

        Args:
            payload (Any): The database cursor object (e.g., from sqlite3, psycopg2, snowflake).
                           Must have already executed a SELECT query.

        Raises:
            TypeError: If the payload is not a valid cursor object.
            ValueError: If the cursor has no description (e.g., no query was executed).

        Returns:
            list[dict]: A list where each dictionary is a row of data, with column headers as keys.
        """
        cursor = payload

        # Ensure it acts like a standard DB-API 2.0 cursor
        if not hasattr(cursor, 'description') or not hasattr(cursor, 'fetchall'):
            raise TypeError("Invalid Payload: Expected a database cursor object.")

        # Check if a query was actually run
        if cursor.description is None:
            raise ValueError("Cursor has no description. Ensure a SELECT query was executed.")

        # Extract the column headers from the cursor description
        # description returns a tuple of tuples where the first item is the column name
        columns = [column[0] for column in cursor.description]

        # Fetch all the raw row tuples
        rows = cursor.fetchall()

        # Zip the headers and the rows together into dictionaries
        return [dict(zip(columns, row)) for row in rows]