from anaplan_orm.client import AnaplanClient, Authenticator
from anaplan_orm.exceptions import AnaplanConnectionError

class DummyAuthenticator(Authenticator):
    def get_auth_headers(self) -> dict:
        print("--> Authenticator called: Generating fake headers...")
        return {"Authorization": "AnaplanAuthToken ThisIsAFakeToken123"}

print("--> Initializing AnaplanClient...")
mock_auth = DummyAuthenticator()
client = AnaplanClient(authenticator=mock_auth, verify_ssl=False)

print("--> Pinging Anaplan API...")

try:
    status_code = client.ping()
    print(f"\n✅ Result: The Anaplan API responded with HTTP Status: {status_code}")
except AnaplanConnectionError as e:
    print(f"\n❌ Pipeline Failed: {e}")