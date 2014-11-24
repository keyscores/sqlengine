from apiclient.discovery import build

def main():
    # Build a service object for interacting with the API.
    #api_root = 'http://localhost:8080/_ah/api'
    api_root = 'https://ks-testsql.appspot.com/_ah/api'
    #api = 'helloworld'
    api = 'ks_analytics'
    version = 'v1'
    discovery_url = '%s/discovery/v1/apis/%s/%s/rest' % (api_root, api, version)
    service = build(api, version, discoveryServiceUrl=discovery_url)

    # Get Measure Ids
    response = service.measures().getAll().execute()
    print(response)
    
    measure_id = 4
    response = service.measures().getData(end_date = "2014-06-02", start_date="2014-06-01", id=measure_id, group_by = "ks_date").execute()
    print(response)
    print type(response)
    
    # RESULT2DICT
    code_dict = {}
    data_dict = {}
    code_dict[measure_id] = response
    
    for row in response['return_list']:
        group_by = row["group_by"]
        value = row["value"]
        data_dict[group_by] = value
    code_dict[measure_id] = data_dict
    print code_dict
    

if __name__ == '__main__':
  main()