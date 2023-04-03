# MLHadoop
This repository contains Machine-Learning MapReduce codes for Hadoop which are written from scratch (without using any package or library). So you'll find codes written right from the basic Mathematics required for all of these Algorithms.
e.g. Prediction Algorithms (Linear and Logistic Regression - Iterative Version), Clustering Algorithm (K-Means Clustering), Classification Algorithm (KNN Classifier), MBA, Common Friends etc.

NOTE: I think some of the algorithms implemented here can be improved in time as well as space by controlling the shuffle-sort phase between a MapReduce job i.e by writing and implementing your own custom Secondary Sort class as the shuffle-sort phase takes up a lot of time. If you have a sort order of key-value pairs in mind and if you are running multiple jobs or extra sorting methods inside mappers and reducers just to get the correct sort order, then, secondary sorting might come in handy as it will speed up the jobs and will use lesser RAM.

Language used: Java

IDE used: Eclipse IDE with [HDT (Hadoop Development Tools)](https://archive.apache.org/dist/incubator/hdt/hdt-0.0.2.incubating/hdt-0.0.2.incubating-bin.tar.gz) plugin installed.

Hadoop version used: 1.2.1

I wrote these codes when I was just a novice (in terms of MapReduce programming as well as programming in general) and therefore I am certain the code is very inefficient and there are a lot of optimisations yet to be done in this. So feel free to point out the mistakes or create PRs if you are interested.

License
Copyright © 2023 [Punit Naik](https://github.com/punit-naik)

This program and the accompanying materials are made available under the terms of the Eclipse Public License 2.0 which is available at http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary Licenses when the conditions for such availability set forth in the Eclipse Public License, v. 2.0 are satisfied: GNU General Public License as published by the Free Software Foundation, either version 2 of the License, or (at your option) any later version, with the GNU Classpath Exception which is available at https://www.gnu.org/software/classpath/license.html.
