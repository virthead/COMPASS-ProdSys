from django.apps import AppConfig

class ProdSysConfig(AppConfig):
    name = 'prodsys'
    verbose_name = "COMPASS ProdSys"
    
class SchedConfig(AppConfig):
    name = 'schedconfig'
    verbose_name = "COMPASS SchedConfig"

class CelerytasksConfig(AppConfig):
    name = 'celerytasks'