from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from starlette.responses import RedirectResponse
import rasterio
import uvicorn
import time
import os


class GribAPI:
    def __init__(self, data_dir = "./data_dir", debug=False):
        self.data_dir = data_dir
        self.datasets = self._get_available_data()
        self.description = {}
        self.debug = debug

        if len(self.datasets) != 0:
            for i in self.datasets:
                self.description[i] = self._parse_available_bands(i) 

    def _get_available_data(self):
        '''Get current available data located in self.data_dir
        '''
        return os.listdir(self.data_dir)

    def _parse_available_bands(self, dataset):
        with rasterio.open(os.path.join(self.data_dir,dataset)) as ds:
            band_amount = ds.count
            all_bands = {}
            for band in range(1,(band_amount+1)):
                band_metadata = ds.tags(band)
                all_bands[band] = band_metadata
        return all_bands

    def _get_point_data(self, dataset, band, lat, lon):
        '''Returns data for given dataset and coordinates
        '''
        if lon < 0:       
            lon = 360 + lon

        with rasterio.open(os.path.join(self.data_dir,dataset)) as ds:
            row, col = ds.index(lon, lat)
            data_value = ds.read(band)[row][col]
        return data_value

    def start(self, host="0.0.0.0", port=8000):

        app = FastAPI(
            title="Grib API",
            description='Access grib data directly via API',
            version=1.0,
            redoc_url=None
        )
        app.add_middleware(GZipMiddleware, minimum_size=1000)

        @app.get("/")
        def index():
            return RedirectResponse(url="/docs")
        
        @app.get("/getLayers")
        def get_layers():
            '''Returns data located in given data_dir
            '''
            return self.description

        @app.get("/getData")
        def get_data(dataset:str, band:int, lat:float, lon:float):
            '''Returns data for given dataset, band and coordinates
            '''
            
            if lat < -90 or lat > 90:
                return {"error":"lat must be between -90 and 90"}
            if lon > 360 or lon < -180:
                return {"error":"lon must be between -180 and 180 or between 0 and 360"}

            try:
                start = time.time()
                value = self._get_point_data(dataset, band, lat, lon)                
                if self.debug:
                    print("took", (time.time() - start) * 1000, "milliseconds")                
                resp = {
                            "request":{
                                "dataset":dataset,
                                "band":band
                            },
                            "data":value
                }
                return resp
            except Exception as e:
                return str(e)

        uvicorn.run(app, host=host, port=port)