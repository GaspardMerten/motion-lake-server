#  Copyright (c) 2024. Gaspard Merten
#  All rights reserved.

import json

import numpy as np


class NumpyEncoder(json.JSONEncoder):
    """
    A custom json.dumps JSONEncoder that encodes numpy types to their native python types
    """

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()

        return json.JSONEncoder.default(self, obj)
