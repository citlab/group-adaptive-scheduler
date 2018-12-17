class JobGroupData:
    group1 = ["WordCount", "KMeans", "LinearRegression"]
    group2 = ["LogisticRegression", "SVM"]
    group3 = ["SortedWordCount", "PageRank"]
    group4 = ["TPCH18"]
    group5 = ["Sort"]
    group6 = ["ConnectedComponent"]
    groups = [group1, group2, group3, group4, group5, group6]

    groupIndexes = {"WordCount": 0, "KMeans": 0, "LinearRegression": 0,
                    "LogisticRegression": 1, "SVM": 1,
                    "SortedWordCount": 2, "PageRank": 2,
                    "TPCH18": 3,
                    "Sort": 4,
                    "ConnectedComponent": 5}

    def get_group_name(self, group_index):
        return self.groups[group_index].__str__()

    # Cluster slots
    SLOT_1 = "slot1"
    SLOT_2 = "slot2"

    cluster_slots_index = {"wally081.cit.tu-berlin.de": SLOT_1,
                           "wally082.cit.tu-berlin.de": SLOT_1,
                           "wally083.cit.tu-berlin.de": SLOT_1,
                           "wally084.cit.tu-berlin.de": SLOT_1,
                           "wally085.cit.tu-berlin.de": SLOT_2,
                           "wally086.cit.tu-berlin.de": SLOT_2,
                           "wally087.cit.tu-berlin.de": SLOT_2,
                           "wally088.cit.tu-berlin.de": SLOT_2, }
