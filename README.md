
This is a fork of the JGroups framework detailed below.  The purpose of this fork is to hold prototype implementations of my research into atomic broadcast protocols.  More details about my research can be found on my linkedin page that is provided on my github profile.  

This fork is NOT actively maintained, and is instead provided as a history of my research and the associated performance evaluations.  If you want to contribute to the JGroups framework you should fork the original repository: https://github.com/belaban/JGroups.

Code directly related to my research
====================================================

The Hybrid atomic broadcast protocol - ABcast (Aramis and Base broadcast) - can be found at [src/protocols/aramis](https://github.com/ryanemerson/JGroups-HiTab/tree/ABService/src/org/jgroups/protocols/aramis)

The protocols used for implementing and interacting with the AmaaS ordering service can be found at [src/protocols/abaas/](https://github.com/ryanemerson/JGroups-HiTab/tree/ABService/src/org/jgroups/protocols/abaas)

The various experiments utilised throughout my resarch can be found at [tests/ABService](https://github.com/ryanemerson/JGroups-HiTab/tree/ABService/tests/ABService/org/jgroups/tests/ABService) and [tests/ProbingValidation](https://github.com/ryanemerson/JGroups-HiTab/tree/ABService/tests/ProbingValidation/org/jgroups/tests/probing_validation)

Bash and Python scripts for deploying, maintaining and analyzing my experiments can be found at [testScripts/](https://github.com/ryanemerson/JGroups-HiTab/tree/ABService/testScripts)

Finally, various configuration files utilised in my experiments can be found at [conf/](https://github.com/ryanemerson/JGroups-HiTab/tree/ABService/conf)


       JGroups - A Framework for Group Communication in Java
       ========================================================

			    March 3, 1998

			       Bela Ban
			   4114 Upson Hall
			  Cornell University
			   Ithaca, NY 14853
			  bba@cs.cornell.edu
		       belaban@yahoo.com


JGroups is a Java library for reliable group communication. It
consists of 3 parts: (1) a socket-like API for application
development, (2) a protocol stack, which implements reliable
communication, and (3) a set of building blocks, which give the
developer high-level abstractions (e.g. ReplicatedHashMap, an
implementation of java.util.Map).

The API (a channel) looks like a socket: there are methods for joining
and leaving groups, sending and receiving messages,
getting the shared group state, and registering for notifications when
a member joins, or an existing member leaves or crashes.

The protocol stack is a list of protocols, through which each
message passes. Each protocol implements an up() and down()
method, and may modify, reorder, encrypt, fragment/unfragment, drop,
or pass a message up/down. The protocol stack is created
according to a specification given when a channel is created. New
protocols can be plugged into the stack easily.

Building blocks hide the channel and provide a higher abstraction.
Example: ReplicatedHashMap implements java.util.Map and implements
all methods that change the map (clear(), put(), remove()).
Those methods are invoked on all hashmap instances in the same group
simultaneously, so that all hashmaps have the same state.
A new hashmap uses a state transfer protocol to initially obtain
the shared group state from an existing member. This allows for 
replication of data structures across processes.



Group communication is important in the following situations:

 - A service has to be replicated for availability. As long as at
   least one of the servers remains operational, the service itself
   remains operational

 - Service requests have to be balanced between a set of servers

 - A large number of objects have to be managed as one entity (e.g. a
   management domain)

 - Notification service / push technology: receivers subscribe to a
   channel, senders send data to the channels, channels distribute
   data to all receivers subscribed to the channel.
   Used for example for video distribution, videoconferencing
	
	
JGroups deliberately models a rather low-level message-oriented
middleware (MOM) model. The reason is that we don't want to impose a
one-size-fits-all model on the programmer, who usually will want to
extend the model in various (previously unconceived) ways anyway.

Providing low level Java classes allows the programmer to
extend/replace classes at will, as the granularity of the system is
finer.




