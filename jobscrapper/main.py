import requests
import src
import argparse

def main():    
    parser = argparse.ArgumentParser(prog="jobscrapper")
    parser.add_argument("mode", default="scrap", choices=["scrap","worker", "insight"])
    parser.add_argument("--config", type=str, default="config.json")
    args = parser.parse_args()
    
    print(f"using config : {args.config}")
    
    if args.mode == "scrap":
        jobs = src.Jobscrapper(args.config)
        jobs.run()
        
    elif args.mode == "worker":
        worker = src.Worker(args.config)
        worker.run()    
    
    elif args.mode  =="insight":
        insight = src.Insight(args.config)
        insight.run()

if __name__ == "__main__":
    main()