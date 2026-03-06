import requests
import sqlite3

def create_table():
    connection=sqlite3.connect('github_data.db')
    try:
        cursor=connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS github_repositories (
            repo_id INTEGER PRIMARY KEY ,
            repo_name TEXT NOT NULL,
            owner TEXT NOT NULL,
            stargazers INTEGER NOT NULL,         
            forks_count INTEGER NOT NULL,
            language TEXT NOT NULL,
            repo_year TEXT NOT NULL,
            html_url TEXT NOT NULL
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
    except sqlite3.Error as e:
        print("error while creating database:{e}")
    finally:
        connection.close()

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
        return all_data

    except:
        print(f"error occurred in the API fetching")

# def check_duplicates(data):
#      seen_repo_ids=set()
#      clean_architecture=[]
#      for check in data:
#          seen_repo_ids.add(check['repo_id'])
#      for repo in data:
#          if repo['repo_id']  in seen_repo_ids:
#              repo['create_at']=repo.get('create_at',{})[0:4]
#              repo['language']=repo.get('language')or 'unknown'
#              print(repo['language'])
#              clean_architecture.append(repo)
#      print(f"Total Records collected: {len(seen_repo_ids)} ,unique records: {len(clean_architecture)} processed,first 2 records:{clean_architecture[0:2]}",)

#      return clean_architecture
def check_duplicates(data):
    seen_repo_ids = set()
    clean_repositories = []

    for repo in data:
        repo_id = repo['repo_id']

        if repo_id not in seen_repo_ids:   # check first
            seen_repo_ids.add(repo_id)     # then mark as seen

            # transformations
            repo['create_at'] = repo.get('create_at', '')[0:4]
            repo['language'] = repo.get('language') or 'unknown'

            clean_repositories.append(repo)

    print(f"Total Records collected: {len(data)}, unique records: {len(clean_repositories)}")
    # print("First 2 records:", clean_repositories[:2])

    return clean_repositories
   
def data_ingestion(data):
    connection=sqlite3.connect('github_data.db')
    bulk_data=[]
    try:
      cursor=connection.cursor()
      for value in data:
              repo_id=value['repo_id']
              repo_name=value['repo_name']
              owner=value['owner']
              stargazers=value['stargazers']
              forks_count=value['forks_count']
              language=value['language']
              repo_year=value['create_at']
              html_url=value['html_url']
              results=(repo_id,repo_name,owner,stargazers,forks_count,language,repo_year,html_url)
            # bulk_data.extend(results)
              bulk_data.append(results)
      insert_query="""insert into github_repositories(repo_id,repo_name,owner,stargazers,forks_count,language,repo_year,html_url)
                       values(?,?,?,?,?,?,?,?)"""
      cursor.executemany(insert_query,bulk_data) 
      connection.commit() 
      print("successfully inserted all the data")
    except sqlite3.Error as e:
        print(f"error occurred:{e}")
    finally:
        connection.close()


#               repo_id=value['repo_id']
#               repo_name=value['repo_name']
#               owner=value['owner']
#               stargazers=value['stargazers']
#               forks_count=value['forks_count']
#               language=value['language']
#               create_dt=value['create_at']
#               html_url=value['html_url']
#               results=(repo_id,repo_name,owner,stargazers,forks_count,language,create_dt,html_url)

if __name__=="__main__":
    create_table()
    page=1
    all_repositories=[]
    while(page<=5):
        data=fetch_api(page)
        all_repositories.extend(data)
        print(f"Page{page} fetched {len(data)}")
        page=page+1
    print(f"Total repositories fetch:{len(all_repositories)}")
    new_data= check_duplicates(all_repositories)
    data_ingestion(new_data)
    