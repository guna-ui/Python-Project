import requests

api='https://api.github.com/search/repositories?q=data+engineering&sort=stars&page=1'

def fetch_api():
    all_data=[]
    try:
        response=requests.get(api)
        response.raise_for_status()
        total_count=response.json()['total_count']
        items=response.json()['items']
        for results in items:
            repo_data={
                "repo_id":results['id'],
                "repo_name":results['name'],
                "owner":results['owner']['login'],
                "stargazers":results['stargazers_count'],
                "forks_count":results['forks_count'],
                "language":results['language'],
                "create_at":results['created_at'],
                "html_url":results['html_url']              
           } 
            all_data.append(repo_data)   
        return all_data

    except:
        print(f"error occurred in the API fetching")



if __name__=="__main__":
    data=fetch_api()
    print(f"Total repositories fetched:{len(data)}")