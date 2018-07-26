import settings
import torch
import torchvision

def loadmodel(hook_fn):
    checkpoint = torch.load(settings.MODEL_FILE, map_location='cpu')
    # only state_dict() saved, model class info is needed
    if type(checkpoint).__name__ == 'OrderedDict' or type(checkpoint).__name__ == 'dict':
        model = torchvision.models.__dict__[settings.MODEL](num_classes=settings.NUM_CLASSES)
        if settings.MODEL_PARALLEL:
            state_dict = {str.replace(k, 'module.', ''): v for k, v in checkpoint[
                'state_dict'].items()}  # the data parallel layer will add 'module' before each layer name
        else:
            state_dict = checkpoint
        model.load_state_dict(state_dict)
    # all info about model was saved
    else:
        model = checkpoint
    # hook register
    for name in settings.FEATURE_NAMES:
        model._modules.get(name).register_forward_hook(hook_fn)
    if settings.GPU:
        model.cuda()
    model.eval()
    return model
