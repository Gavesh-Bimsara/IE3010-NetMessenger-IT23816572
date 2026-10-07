## Design Diary — NetMessenger (IT23816572)

* **Architecture decision:** I decided to use a -based single-process server instead of using threads or multiple processes with . Since the server needs to manage shared information such as the user list and chat rooms, I felt that using a single process would make the design simpler and avoid the need for locks. It was also easier for me to understand and implement within the available time.

* **Protocol gap identified:** While working through the specification, I noticed that it does not clearly explain how the recipient should receive and identify an incoming file. I decided to use the same framing format as the sender's  command, which is  followed by the raw file bytes. I chose this approach because it keeps the file-transfer format consistent.

* **Obstacle:** One issue I initially overlooked was what should happen when a  request is rejected because the file is too large. I first considered simply rejecting the request, but this would leave the file bytes in the TCP stream. Those bytes could then be interpreted as the next command and break the protocol. I fixed this by continuing to read and discard the remaining file bytes so that the connection stays synchronized.

* **Obstacle:** I also found an issue with the client when the standard input reached EOF. The client could exit before receiving the server's  response after sending . I fixed this by keeping the socket open and continuing to read from the server until the expected response had been received.

* **Testing approach:** I created an automated test harness using raw sockets to test all 10 protocol commands, including the different error conditions. This allowed me to test the server consistently and identify problems early. After that, I also tested the system using the actual compiled client programs with five clients connected at the same time. I performed a real file transfer and used  to confirm that the received file was exactly the same as the original.

* **Personalisation:** I generated the required port number and other names from my registration number, . This resulted in port  and NID , along with the required file, log, and storage names. I also verified that the server was actually listening on my assigned port using  and later confirmed it using State  Recv-Q Send-Q Local Address:Port    Peer Address:PortProcess                             
LISTEN 0      4096       127.0.0.1:631          0.0.0.0:*                                       
LISTEN 0      128          0.0.0.0:22           0.0.0.0:*                                       
ESTAB  0      0          10.0.2.15:48654  140.82.114.25:443  users:(("firefox",pid=3605,fd=177))
ESTAB  0      0          10.0.2.15:55740 172.64.148.235:443  users:(("firefox",pid=3605,fd=75)) 
ESTAB  0      0          10.0.2.15:53988  160.79.104.10:443  users:(("firefox",pid=3605,fd=87)) 
ESTAB  0      0          10.0.2.15:49752 172.64.155.209:443  users:(("firefox",pid=3605,fd=95)) 
ESTAB  0      0          10.0.2.15:32858  34.107.243.93:443  users:(("firefox",pid=3605,fd=109))
LISTEN 0      128             [::]:22              [::]:*                                       
LISTEN 0      4096               *:9090               *:*                                       
LISTEN 0      4096           [::1]:631             [::]:*                                        on my VM.

