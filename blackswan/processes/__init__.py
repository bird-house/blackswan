from .wps_say_hello import SayHello
from .wps_analogs_reanalyse import AnalogsreanalyseProcess
from .wps_analogs_model import AnalogsmodelProcess
from .wps_analogs_compare import AnalogscompareProcess
from .wps_pythonanattribution import PythonanattributionProcess
from .wps_eventattrib import EventAttributionProcess
from .wps_localdims_rea import LocaldimsReaProcess
processes = [
    SayHello(),
    AnalogsreanalyseProcess(),
    AnalogsmodelProcess(),
    AnalogscompareProcess(),
    PythonanattributionProcess(),
    EventAttributionProcess(),
    LocaldimsReaProcess(),
]
