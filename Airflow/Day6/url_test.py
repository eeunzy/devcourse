import requests

def get_restcountries():
    
    url = 'https://restcountries.com/v3.1/all'    
    try:
        response = requests.get(url, timeout=30)  
        response.raise_for_status()
        json_data = response.json()  # 또는 response.text 혹은 다른 방식으로 처리

        records = []
        for country in json_data:
            name = country["name"]["official"]
            population = country["population"]
            area = country["area"]
            records.append([name, population, area])
        
        for record in records[:5]:
            print(f"name: %s, population: %s, area: %s ",record)
        

    except requests.exceptions.RequestException as e:
        print("요청 중 오류 발생:", e)

get_restcountries()