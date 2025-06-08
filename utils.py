
import requests

import pandas as pd
def get_projects_mapping(url_base):
    """
    Fetches all projects from a paginated API endpoint and builds mapping dictionaries.
    """
    project_id_to_warehouseId = {}
    project_id_to_issuances = {}
    project_id_to_locationId = {}
    duplicate_projects = {}
    project_id_to_locations = {}

    page = 1
    limit = 1000  # Adjust as needed
    while True:
        url = f"{url_base}&page={page}&limit={limit}"
        print(url)
        response = requests.get(url)
        print("all good")
        if response.status_code != 200:
            print(f"Failed to fetch data: {response.status_code}")
            break

        print(response)
        # print(response.text)
        
        data = response.json()
        # Support both paginated and non-paginated API responses
        projects = data.get("data", data) if isinstance(data, dict) else data

        for project_data in projects:
            if (project_data.get("orgUid") == "b3d4e71d806e86ff1f8712b6854d65e2c178e873ee22b2f7d0da937dacbaa985" and
                project_data.get("registryOfOrigin") == "CDM Registry"):

                warehouseId = project_data.get("warehouseProjectId")
                projectId = project_data.get("projectId")
                # Mapping of 'projectId' and 'warehouseProjectId'
                if projectId not in project_id_to_warehouseId:
                    project_id_to_warehouseId[projectId] = warehouseId
                else:
                    duplicate_projects[projectId] = [project_id_to_warehouseId[projectId], warehouseId]
                    project_id_to_warehouseId[projectId] = warehouseId

                # Mapping of 'projectId' and 'issuance' if 'issuance' exists
                if isinstance(project_data.get('issuances'), list):
                    issuances_for_api = []
                    for issuance in project_data['issuances']:
                        issuances_for_api.append({key: value for key, value in issuance.items()})
                    if issuances_for_api:
                        project_id_to_issuances[projectId] = issuances_for_api

                # Mapping of 'projectId' and 'id' of 'projectLocations'
                if project_data.get("projectLocations"):
                    project_id_to_locationId[projectId] = project_data["projectLocations"][0]["id"]
                    project_id_to_locations[projectId] = project_data["projectLocations"]

        # Pagination handling
        if isinstance(data, dict) and "pageCount" in data:
            if page >= data["pageCount"]:
                break
        else:
            # If not paginated, break after first fetch
            break
        page += 1

    return project_id_to_warehouseId, project_id_to_issuances, project_id_to_locationId, duplicate_projects, project_id_to_locations

def get_all_projects_as_dataframe(url_base):
    """
    Fetches all projects from a paginated API endpoint and returns them as a pandas DataFrame.
    """
    all_projects = []
    page = 1
    limit = 1000  # Adjust as needed

    while True:
        url = f"{url_base}&page={page}&limit={limit}"
        print(f"Fetching: {url}")
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch data: {response.status_code}")
            break

        data = response.json()
        projects = data.get("data", data) if isinstance(data, dict) else data

        if not projects:
            break

        all_projects.extend(projects)

        # Pagination handling
        if isinstance(data, dict) and "pageCount" in data:
            if page >= data["pageCount"]:
                break
        else:
            break
        page += 1

    df = pd.DataFrame(all_projects)
    print(f"Fetched {len(df)} projects")
    return df