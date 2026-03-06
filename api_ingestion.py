import requests


def fetch_api(page):
    all_data=[]
    try:      
        api=f'https://api.github.com/search/repositories?q=data+engineering&sort=stars&page={page}'
        response=requests.get(api)
        response.raise_for_status()
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
        # page=page+1
        return all_data

    except:
        print(f"error occurred in the API fetching")



if __name__=="__main__":
    page=1
    total_count=0
    while(page<=5):
     data=fetch_api(page)
     total_count=total_count+len(data)
     print(f"Page{page} fetched {len(data)}")
     page=page+1
    print(f"Total repositories fetch:{total_count}")