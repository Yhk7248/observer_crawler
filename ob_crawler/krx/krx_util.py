def flatten_dict(d):
    return {k: v[0] if len(v) == 1 else v for k, v in d.items()}