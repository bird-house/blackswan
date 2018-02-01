from .wps_weatherregimes_reanalyse import WeatherregimesreanalyseProcess
#from .wps_weatherregimes_projection import WeatherregimesprojectionProcess
from .wps_weatherregimes_model import WeatherregimesmodelProcess
from .wps_analogs_reanalyse import AnalogsreanalyseProcess
from .wps_analogs_model import AnalogsmodelProcess
from .wps_analogs_compare import AnalogscompareProcess
from .wps_analogs_viewer import AnalogsviewerProcess

processes = [
    WeatherregimesreanalyseProcess(),
#    WeatherregimesprojectionProcess(),
    WeatherregimesmodelProcess(),
    AnalogsreanalyseProcess(),
    AnalogsmodelProcess(),
    AnalogscompareProcess(),
    AnalogsviewerProcess(),
]
