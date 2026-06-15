from google.cloud import secretmanager


def get_secret(project_id, secret_id, version="latest"):
    """
    Retrieve a secret from Google Secret Manager.
    """

    client = secretmanager.SecretManagerServiceClient()

    secret_name = (
        f"projects/{project_id}/secrets/"
        f"{secret_id}/versions/{version}"
    )

    response = client.access_secret_version(
        request={"name": secret_name}
    )

    return response.payload.data.decode("UTF-8")
    if __name__ == "__main__":

    project_id = "your-project-id"
    secret_id = "api-key"

    secret = get_secret(project_id, secret_id)

    print(secret)