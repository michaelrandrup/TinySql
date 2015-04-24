# TinySQL 0.1B
Small and fast object-oriented SQL statement builder

TinySQL is a light-weight object wrapper around the SQL syntax.

If you do not want or need EF or other frameworks, you can use TinySQL to wrap SQL statement in an object-oriented and fluid way.

For the SQL language, the scope for TinySQL is to be able to:
1. Create SELECT statements
2. Create UPDATE statements
3. Create INSERT statements
4. Create DELETE statements

For working with data results, the scope is to:
1. Provide an object-oriented wrapper around result sets
2. Be able to generate POCO classes from SQL results
3. Be able to generate strongly typed Lists (List<T>) and dictionaries (Dictionary<TKey,TValue>) from results

for working with data, the scope is to:
1. Provide batched insert, updates and deletes
2. Provide transactional safety

for developers, the scope is:
1. To be light-weight without any requirements or constraints for adding TinySQL to a project
2. To be Extendible via Extension Methods and partial classes
