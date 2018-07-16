features_blobs = []
def hook_feature(module, input, output):
    features_blobs.append(output.data.cpu().numpy())