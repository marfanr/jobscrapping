import requests
import src
import argparse

def main():    
    parser = argparse.ArgumentParser(prog="jobscrapper")
    subparser = parser.add_subparsers(required=True, dest="mode")
    
    worker_args = subparser.add_parser("worker")
    worker_args.add_argument("--config", type=str, default="config.json")
    
    scrap_args = subparser.add_parser("scrap")
    scrap_args.add_argument("--config", type=str, default="config.json")
    
    olap_args = subparser.add_parser("olap")
    olap_args.add_argument("--config", type=str, default="config.json")
    olap_args.add_argument("--task", type=str, required=True)
    
    
    args = parser.parse_args()
    
    print(f"using config : {args.config}")
    
    if args.mode == "scrap":
        jobs = src.Jobscrapper(args.config)
        jobs.run()
        
    elif args.mode == "worker":
        worker = src.Worker(args.config)
        worker.run()    
    
    elif args.mode == "olap":
        olap = src.Olap(args.config)
        olap.run(args.task)

if __name__ == "__main__":
    main()