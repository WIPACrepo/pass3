import argparse

import globus_sdk
from globus_sdk.globus_app import UserApp
from globus_sdk.scopes import TransferScopes

def login_and_get_transfer_client(auth_client, scopes=TransferScopes.all):
    auth_client.oauth2_start_flow(requested_scopes=scopes)
    authorize_url = auth_client.oauth2_get_authorize_url()
    print(f"Please go to this URL and login:\n\n{authorize_url}\n")

    auth_code = input("Please enter the code here: ").strip()
    tokens = auth_client.oauth2_exchange_code_for_tokens(auth_code)
    transfer_tokens = tokens.by_resource_server["transfer.api.globus.org"]

    # return the TransferClient object, as the result of doing a login
    return globus_sdk.TransferClient(
        authorizer=globus_sdk.AccessTokenAuthorizer(transfer_tokens["access_token"])
    )

def do_submit(client):
    task_doc = client.submit_transfer(task_data)
    task_id = task_doc["task_id"]
    print(f"submitted transfer, task_id={task_id}")

def submit_transfer(transfer_client: globus_sdk.TransferClient,
                    SRC_COLLECTION,
                    DST_COLLECTION,
                    SRC_PATH,
                    DST_PATH):
    # Comment out each of these lines if the referenced collection is either
    #   (1) A guest collection or (2) high assurance.
    transfer_client.add_app_data_access_scope(SRC_COLLECTION)
    transfer_client.add_app_data_access_scope(DST_COLLECTION)

    transfer_request = globus_sdk.TransferData(SRC_COLLECTION, DST_COLLECTION)
    transfer_request.add_item(SRC_PATH, DST_PATH)

    task = transfer_client.submit_transfer(transfer_request)
    print(f"Submitted transfer. Task ID: {task['task_id']}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--src',
                        help="source globus endpoint uuid. default is TACC Ranch",
                        type=str,
                        default="57c4032a-2b50-47f0-adf8-13fff3a7d77d")
    parser.add_argument("--dst",
                        help="dest globus endpoint uuid. Default is TACC Vista",
                        type=str,
                        default="fcb0b578-dcb3-4043-a841-8bd0974d6af1")
    parser.add_argument("--app",
                        help="globus app uuid",
                        type=str,
                        default="24e70a5d-071b-4452-8e9d-d7d460f42851"# "438ca41e-d2d9-493e-8631-91a8c475b629"
                        )
    parser.add_argument("--srcdir",
                        help="source dir",
                        type=str,
                        required=True)
    parser.add_argument("--destdir",
                        help="dest dir",
                        type=str,
                        required=True)
    args=parser.parse_args()

    with UserApp("my-simple-transfer", client_id=args.app) as app:
        with globus_sdk.TransferClient(app=app) as client:
            submit_transfer(client, args.src, args.dst, args.srcdir, args.destdir)


    #auth_client = globus_sdk.NativeAppAuthClient(args.app)
    #transfer_client = login_and_get_transfer_client(auth_client)

    #task_data = globus_sdk.TransferData(
    #    source_endpoint=args.src,
    #    destination_endpoint=args.dst)

    #task_data.add_item(
    #    args.srcdir,
    #    args.destdir
    #)

    #try:
    #    do_submit(transfer_client)
    #except globus_sdk.TransferAPIError as err:
    #    # if the error is something other than consent_required, reraise it,
    #    # exiting the script with an error message
    #    if not err.info.consent_required:
    #        raise

    #    # we now know that the error is a ConsentRequired
    #    # print an explanatory message and do the login flow again
    #    print(
    #        "Encountered a ConsentRequired error.\n"
    #        "You must login a second time to grant consents.\n\n"
    #    )
    #    transfer_client = login_and_get_transfer_client(auth_client,
    #        scopes=err.info.consent_required.required_scopes
    #    )

    #    # finally, try the submission a second time, this time with no error
    #    # handling
    #    do_submit(transfer_client)
