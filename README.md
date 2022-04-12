# dlmanage - An optinionated Tool to manage small deeplearn Clusters with slurm

**Disclaimer**: This tool is build to manage the accounts, nodes, ressources and jobs at Stuttgart Media Universitys Deeplearning Cluster and may not fit your model how you want to manage these ressources.

This is a simple TUI that allows to quickly add users and accounts and associate CPU, GPU and Time ressources to those. It also allows to manage the limits of running jobs and manage the state of nodes. The Account Management uses the following assumptions:

- Each Account only has one parent max
- Each User belongs to one Account at most
- Limits on Accounts limit the ressources all users in this account can use simultaneously

These assumptions allow the admin to quickly change the associations in a table like structure without having to construct long `sacctmgr` commands. The tool uses the commands `sacctmgr`, `scontrol` and `scancel` as interface to the slurm cluster, therefore the user needs to have the right to use those commands to issue all actions. if the user has no admin rights, he can still use the tool to view the current state of the cluster and hist jobs.
