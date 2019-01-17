class JobGroupData:
    group1 = ["WordCount", "KMeans", "LinearRegression"]
    group2 = ["LogisticRegression", "SVM"]
    group3 = ["SortedWordCount", "PageRank"]
    group4 = ["TPCH"]
    group5 = ["Sort"]
    group6 = ["ConnectedComponent"]
    groups = [group1, group2, group3, group4, group5, group6]

    groupIndexes = {"WordCount": 0, "KMeans": 0, "LinearRegression": 0,
                    "LogisticRegression": 1, "SVM": 1,
                    "SortedWordCount": 2, "PageRank": 2,
                    "TPCH": 3,
                    "Sort": 4,
                    "ConnectedComponent": 5}
    group_names = ["WC,KM,LiR", "LoR,SVM", "SWC,PR", "TPCH", "S", "CC"]

    def get_group_name(self, group_index):
        return self.group_names[group_index]

    # Cluster slots
    SLOT_1 = "slot1"
    SLOT_2 = "slot2"
    SLOT_FULL = "slot1|slot2"

    cluster_slots_index = {"wally060.cit.tu-berlin.de": SLOT_1,
                           "wally061.cit.tu-berlin.de": SLOT_1,
                           "wally062.cit.tu-berlin.de": SLOT_1,
                           "wally063.cit.tu-berlin.de": SLOT_1,
                           "wally064.cit.tu-berlin.de": SLOT_1,
                           "wally065.cit.tu-berlin.de": SLOT_1,
                           "wally066.cit.tu-berlin.de": SLOT_1,
                           "wally067.cit.tu-berlin.de": SLOT_1,
                           "wally068.cit.tu-berlin.de": SLOT_1,
                           "wally069.cit.tu-berlin.de": SLOT_1,
                           "wally071.cit.tu-berlin.de": SLOT_1,
                           "wally072.cit.tu-berlin.de": SLOT_1,
                           "wally073.cit.tu-berlin.de": SLOT_1,
                           "wally075.cit.tu-berlin.de": SLOT_1,
                           "wally076.cit.tu-berlin.de": SLOT_1,
                           "wally077.cit.tu-berlin.de": SLOT_1,
                           "wally078.cit.tu-berlin.de": SLOT_2,
                           "wally079.cit.tu-berlin.de": SLOT_2,
                           "wally081.cit.tu-berlin.de": SLOT_2,
                           "wally082.cit.tu-berlin.de": SLOT_2,
                           "wally083.cit.tu-berlin.de": SLOT_2,
                           "wally084.cit.tu-berlin.de": SLOT_2,
                           "wally085.cit.tu-berlin.de": SLOT_2,
                           "wally086.cit.tu-berlin.de": SLOT_2,
                           "wally087.cit.tu-berlin.de": SLOT_2,
                           "wally088.cit.tu-berlin.de": SLOT_2,
                           "wally089.cit.tu-berlin.de": SLOT_2,
                           "wally090.cit.tu-berlin.de": SLOT_2,
                           "wally091.cit.tu-berlin.de": SLOT_2,
                           "wally092.cit.tu-berlin.de": SLOT_2,
                           "wally093.cit.tu-berlin.de": SLOT_2,
                           "wally094.cit.tu-berlin.de": SLOT_2, }
