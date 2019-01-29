from .wps_say_hello import SayHello
from .wps_analogs_reanalyse import AnalogsreanalyseProcess
from .wps_analogs_model import AnalogsmodelProcess
from .wps_pythonanattribution import PythonanattributionProcess
processes = [
    SayHello(),
    AnalogsreanalyseProcess(),
    AnalogsmodelProcess(),
    PythonanattributionProcess(),
]
