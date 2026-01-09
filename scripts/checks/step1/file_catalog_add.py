async def post_filecatalog(data: dict, client_secret: str):
    client = ClientCredentialsAuth(
        address="https://file-catalog.icecube.aq",
        token_url="https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
        client_id="pass3-briedel",
        client_secret=client_secret,
    )

    print(f"post {data} to file catalog")
    return_val = await client.request("POST", "/api/files", data)
    print(f"return from file catalog {return_val}")
    return data