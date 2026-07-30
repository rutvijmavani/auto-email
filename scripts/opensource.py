import json
import argparse
import os
import time
from pathlib import Path
from collections import defaultdict, Counter


OUTPUT = "output/company_explorer.json"



# -----------------------------
# Reader
# -----------------------------

def read_records(filepath):

    with open(filepath, "r", encoding="utf-8") as f:

        first = f.read(1)
        f.seek(0)

        if first == "[":
            data = json.load(f)

            for r in data:
                yield r

        else:

            for line in f:

                line=line.strip()

                if line:
                    yield json.loads(line)



# -----------------------------
# Feature extraction
# -----------------------------

def extract_features(record):

    features = {}

    for item in record.get("FEATURES", []):

        if isinstance(item, dict):

            for k,v in item.items():

                features[k]=v


    return features



# -----------------------------
# Analyzer
# -----------------------------

class Explorer:


    def __init__(self):

        self.start=time.time()

        self.records=0
        self.files=0


        self.names = defaultdict(set)

        self.addresses = defaultdict(set)

        self.websites = defaultdict(set)

        self.placekeys = defaultdict(set)


        self.name_address = defaultdict(set)


        self.states = Counter()

        self.countries = Counter()


        self.anchor_match = 0

        self.anchor_mismatch = 0



    def process(self, record):


        self.records += 1


        f = extract_features(record)



        bq = str(
            f.get("BQ_ID","")
        )


        if not bq:
            return



        name = f.get(
            "NAME_ORG"
        )


        address = (
            f.get("ADDR_LINE1","")
            + "|"
            + f.get("ADDR_CITY","")
            + "|"
            + f.get("ADDR_STATE","")
        )


        website = f.get(
            "WEBSITE_ADDRESS"
        )


        placekey = f.get(
            "PLACEKEY"
        )



        # -----------------
        # Name duplicates
        # -----------------

        if name:

            self.names[name.upper()].add(bq)



        # -----------------
        # Address reuse
        # -----------------

        if address.strip("|"):

            self.addresses[address].add(bq)



        # -----------------
        # Website reuse
        # -----------------

        if website:

            self.websites[website.lower()].add(bq)



        # -----------------
        # Placekey reuse
        # -----------------

        if placekey:

            self.placekeys[placekey].add(bq)



        # -----------------
        # Name + address
        # -----------------

        if name and address:

            key = (
                name.upper()
                +
                "||"
                +
                address
            )

            self.name_address[key].add(bq)



        # geography

        state=f.get(
            "ADDR_STATE"
        )

        country=f.get(
            "ADDR_COUNTRY"
        )


        if state:

            self.states[state]+=1


        if country:

            self.countries[country]+=1



        # anchor validation

        anchor=f.get(
            "REL_ANCHOR_KEY"
        )


        if anchor:

            if str(anchor)==bq:

                self.anchor_match+=1

            else:

                self.anchor_mismatch+=1



    def summarize(self):


        def duplicates(mapping):

            result=[]

            for key,values in mapping.items():

                if len(values)>1:

                    result.append(
                        {
                            "key":key,
                            "count":len(values),
                            "sample_ids":
                                list(values)[:10]
                        }
                    )

            return sorted(
                result,
                key=lambda x:x["count"],
                reverse=True
            )[:100]



        report={


            "statistics":
            {
                "files":
                    self.files,

                "records":
                    self.records,

                "runtime_seconds":
                    round(
                        time.time()-self.start,
                        2
                    )
            },



            "anchor_check":
            {
                "same_as_bqid":
                    self.anchor_match,

                "different":
                    self.anchor_mismatch
            },



            "duplicate_analysis":
            {

                "same_company_name":
                    duplicates(self.names),


                "same_address":
                    duplicates(self.addresses),


                "same_website":
                    duplicates(self.websites),


                "same_placekey":
                    duplicates(self.placekeys),


                "same_name_and_address":
                    duplicates(self.name_address)

            },



            "geography":
            {

                "countries":
                    self.countries.most_common(20),


                "states":
                    self.states.most_common(30)

            }


        }



        os.makedirs(
            "output",
            exist_ok=True
        )


        with open(
            OUTPUT,
            "w",
            encoding="utf-8"
        ) as f:


            json.dump(
                report,
                f,
                indent=2
            )



        print("\n========== DONE ==========")

        print(
            "Records:",
            self.records
        )

        print(
            "Anchor == BQ_ID:",
            self.anchor_match
        )

        print(
            "Anchor mismatch:",
            self.anchor_mismatch
        )

        print(
            "Output:",
            OUTPUT
        )
    

def print_summary(self):

    def top_duplicates(mapping, title):

        print("\n" + "="*60)
        print(title)
        print("="*60)

        results = []

        for key, values in mapping.items():

            if len(values) > 1:

                results.append(
                    (key, len(values))
                )


        results.sort(
            key=lambda x:x[1],
            reverse=True
        )


        for key,count in results[:10]:

            print(
                f"{count:5} | {key[:120]}"
            )


    print("\n\n######## DISCOVERY SUMMARY ########")


    print("\nRecords:", self.records)


    print("\nAnchor Check")

    print(
        "REL_ANCHOR_KEY == BQ_ID:",
        self.anchor_match
    )

    print(
        "REL_ANCHOR_KEY != BQ_ID:",
        self.anchor_mismatch
    )


    top_duplicates(
        self.names,
        "TOP DUPLICATE COMPANY NAMES"
    )


    top_duplicates(
        self.addresses,
        "TOP SHARED ADDRESSES"
    )


    top_duplicates(
        self.websites,
        "TOP SHARED WEBSITES"
    )


    top_duplicates(
        self.placekeys,
        "TOP SHARED PLACEKEYS"
    )


    top_duplicates(
        self.name_address,
        "TOP SAME NAME + ADDRESS"
    )


    print("\nTop States")

    for state,count in self.states.most_common(15):

        print(
            state,
            count
        )





# -----------------------------
# Main
# -----------------------------

def main():


    parser=argparse.ArgumentParser()


    parser.add_argument(
        "--path",
        required=True
    )


    parser.add_argument(
        "--files",
        type=int,
        default=10
    )


    args=parser.parse_args()



    explorer=Explorer()



    files=sorted(
        Path(args.path).glob("*")
    )[:args.files]



    for file in files:


        print(
            "Processing:",
            file
        )


        explorer.files+=1


        for record in read_records(file):

            explorer.process(record)



    explorer.summarize()




if __name__=="__main__":

    main()