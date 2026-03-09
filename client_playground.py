from anaplan_orm.client import AnaplanClient
from anaplan_orm.client import BasicAuthenticator
from anaplan_orm.exceptions import AnaplanConnectionError

def ping_anaplan():
    # Basic Auth Playground
    print("--> Initializing AnaplanClient...")
    basic_auth = BasicAuthenticator(
        email="xxxx@xxxx.xxx", # change with your Anaplan's account username
        pwd="xxxxxxx", # change with your Anaplan's account password
        verify_ssl=False
    )

    client = AnaplanClient(authenticator=basic_auth, verify_ssl=False)
    print("--> Pinging Anaplan API...")

    try:
        status_code = client.ping()
        print(f"\n✅ Result: The Anaplan API responded with HTTP Status: {status_code}")
    except AnaplanConnectionError as e:
        print(f"\n❌ Pipeline Failed: {e}")

# execute the ping 
ping_anaplan()