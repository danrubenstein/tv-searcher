import argparse

from .warehouse.loading import run_loading_process
from .modeling.model_labeling import get_latest_model
from .labeling.app import run_labeling_app


if __name__ == "__main__":

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    
    parser.add_argument('--model', dest='model', action='store_true')
    parser.set_defaults(model=False)
    
    parser.add_argument('--load', dest='load', action='store_true')
    parser.set_defaults(load=False)

    parser.add_argument('--label', dest='label', action='store_true')
    parser.set_defaults(label=False)

    args = parser.parse_args()

    if args.model:
        get_latest_model()

    if args.load:
        run_loading_process()

    if args.label:
        run_labeling_app()