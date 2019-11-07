from oio.xcute.modules.blob_mover import RawxDecommissionModule
from oio.xcute.modules.tester import TesterModule

MODULE_TYPES = {
    RawxDecommissionModule.MODULE_TYPE: RawxDecommissionModule,
    TesterModule.MODULE_TYPE: TesterModule
}


def get_module_class(job_info):
    module_type = job_info.get('job', dict()).get('type')
    if not module_type:
        raise ValueError('Missing job type')

    module_class = MODULE_TYPES.get(module_type)
    if not module_class:
        raise ValueError('Job type unknown')
    return module_class
