#We can connect to an API, Database, File Reader
import requests

class DataConnectors:

    def __init__(self,data_config):
        self.config = data_config

    def api_connector(self, endpoint: str, limit: int, page_pointer: int):

        """
        TODO: Think about large datasets should we paginate or is this something we can tolerate being a little slow. I think we can run this as async too to avoid blocking other fetches

        Pagination seems like a a good implementation to avoid timeouts/memory issues
            - Things to look at do most API's that fetch data have a row counter or how can we normalize a pointer to last fetched point of data
            - Also how does this work with text? I think it works the same but lets dig int a little 
        """


        req = requests.get(endpoint)
        try:
            json_data = req.json()
            if req.status_code in [500,503,403,404,401,400]:
                raise ValueError("JSON object not returned")

            return json_data
        except Exception as e:
            print("Invalid JSON returned from API, trying as text...", e)
            try:
                text_data = req.text
                return text_data

            except Exception as e2:
                print("Error fetching as text ", e)
                return

    def csv_connector():
        pass

    def db_connector():
        #This in itself might have multiple db configs might have to adress this
        pass
