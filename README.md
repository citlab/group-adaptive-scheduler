## Group Adaptive scheduler

Example command:

```
python3 main.py run test/with_prior_preference_random/config.yaml test/with_prior_preference_random/jobs.xml test/with_prior_preference_random/experiment.xml -s GroupAdaptiveExtend -e GroupGradient -ep estimation_input -eo estimation_output -jtp 8 -rr False -wl 20 -en ex_name> log_name.log 2>&1 &
```

Available parameters:

- config.yaml, jobs.xml, experiment.xml point to the experiment configuration file, job configuration file and the experiment queue file, in that order

- `-s` : name of the scheduler algorithm `[RoundRobin, Adaptive, GroupAdaptive, GroupAdaptiveExtend]`

- `-e` : estimation algorithm `[EpsilonGreedy, Gradient]`

- `-ep` : input preference data folder

- `-eo` : output preference data folder

- `-jtp` : `jobs_to_peek` parameter - number of jobs to consider to schedule in each round

- `-wl` : `waiting_limit` parameter - for considering late job - only used with `GroupAdaptiveExtend` scheduler

- `-rr` : activate random arrival rate
