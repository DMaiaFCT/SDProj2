# README

This project includes a solution for project 1 of Distributed Systems NOVA
FCT, 2025/26. This code can be used as the basis for solving the second
project, but students can use their own version.


## IMPORTANT CHANGES (since initial release)

1) In *JavaMessages.java*, searchInbox, SQL Query was corrected to use **INNER JOIN**, instead of **RIGHT JOIN**.

### To improve support for slower cpus/machines:

1. Timeout values in *RestClient.java* increased from 3000 to 10000ms.
   
2. *Hibernate.getInstance()* is now called explicitly in the servers (REST/GRPC) initial setup code to initialize the database before the first client request.

3. In *hibernate.cfg.xml*, changed **show_sql to **false**.
   
## MINOR CHANGES (since initial release)

1. pom.xml was updated to remove ***smduarte/** from the name of the generated docker image.

## Tester flags that can help in slower machines:

 -sleep 15 -deadline 20000 -interdcdelay 20000



