.. _lineage:

4. Lineage API
==============
The Lineage API is provided by the module ``dataworkspaces.lineage``.

.. automodule:: dataworkspaces.lineage
   :no-undoc-members:

Classes
-------
.. autoclass:: ResourceRef


.. autoclass:: Lineage()
   :members:
   :no-undoc-members:


.. autoclass:: LineageBuilder
   :members:
   :undoc-members:

Using Lineage
-------------
Once you have instrumented the individual steps of your workflow,
you can run the steps as normal. Lineage data is stored in the
directory ``.dataworkspace/current_lineage``, but not checked into
the associated Git repository.

When you take a snapshot, this
lineage data is copied to ``.dataworkspace/snapshot_lineage/HASH``,
where HASH is the hashcode associated with the snapshot,
and checked into git. This data is available as a record of how
you obtained the results associated with the snapshot. In the
future, more tools will be provided to analyze and operate on
this lineage (e.g. replaying workflows).

When you restore a snapshot, the lineage data assocociated 
with the snapshot is restored to ``.dataworkspace/current_lineage``.

Consistency
~~~~~~~~~~~
In order to fully track the status of your workflow, we make a few
restrictions:

1. Independent steps should not overwrite the same ResourceRef or a
   ResourceRef where one ResourceRef refers to the subdirectory of
   another ResourceRef.
2. A step's execution should not transitively depend on two different
   versions of the same ResourceRef. If you try to run a step in this
   situation, an exception will be thrown.

These restrictions should not impact reasonable workflows in practice.
Furthermore, they help to catch some common classes of errors (e.g. not
rerunning all the dependent steps when a change is made to an input).

