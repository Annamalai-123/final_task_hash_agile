const { Client } = require('@elastic/elasticsearch');
const client = new Client({ node: 'http://localhost:9200' });

// Load employee dataset
const employees = require('./employees.json');

async function createCollection(p_collection_name) {
  await client.indices.create({
    index: p_collection_name
  }, { ignore: [400] });  // 400 error means index already exists
  console.log(`Collection ${p_collection_name} created.`);
}

async function indexData(p_collection_name, p_exclude_column) {
  const dataToIndex = employees.map(emp => {
    let { [p_exclude_column]: exclude, ...data } = emp;
    return data;
  });

  for (let i = 0; i < dataToIndex.length; i++) {
    await client.index({
      index: p_collection_name,
      id: dataToIndex[i].EmployeeID,
      body: dataToIndex[i]
    });
  }
  console.log(`Data indexed into ${p_collection_name}, excluding ${p_exclude_column}.`);
}

async function searchByColumn(p_collection_name, p_column_name, p_column_value) {
  const { body } = await client.search({
    index: p_collection_name,
    body: {
      query: {
        match: { [p_column_name]: p_column_value }
      }
    }
  });
  console.log(`Search results for ${p_column_name} = ${p_column_value}:`, body.hits.hits);
}

async function getEmpCount(p_collection_name) {
  const { body } = await client.count({ index: p_collection_name });
  console.log(`Employee count in ${p_collection_name}: ${body.count}`);
}

async function delEmpById(p_collection_name, p_employee_id) {
  await client.delete({
    index: p_collection_name,
    id: p_employee_id
  });
  console.log(`Employee with ID ${p_employee_id} deleted from ${p_collection_name}.`);
}

async function getDepFacet(p_collection_name) {
  const { body } = await client.search({
    index: p_collection_name,
    body: {
      aggs: {
        departments: {
          terms: { field: 'Department.keyword' }
        }
      }
    }
  });
  console.log(`Department Facets:`, body.aggregations.departments.buckets);
}

const v_nameCollection = 'Hash_Annamalai';
const v_phoneCollection = 'Hash_8064';

(async () => {
  await createCollection(v_nameCollection);
  await createCollection(v_phoneCollection);
  await getEmpCount(v_nameCollection);
  await indexData(v_nameCollection, 'Department');
  await indexData(v_phoneCollection, 'Gender');
  await delEmpById(v_nameCollection, 'E02003');
  await getEmpCount(v_nameCollection);
  await searchByColumn(v_nameCollection, 'Department', 'IT');
  await searchByColumn(v_nameCollection, 'Gender', 'Male');
  await searchByColumn(v_phoneCollection, 'Department', 'IT');
  await getDepFacet(v_nameCollection);
  await getDepFacet(v_phoneCollection);
})();
