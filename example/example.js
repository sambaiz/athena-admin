// e.g. TZ=Asia/Tokyo node example.js

const AthenaAdmin = require('../lib/index').AthenaAdmin;
const moment = require('moment');

const utcToTZ = (matched, objKey, table) => {
  let existsDt = false;
  table.partition.keys.forEach((key) => {
    if (key.name === 'dt') {
      existsDt = true;
    }
  });
  if (!existsDt) {
    return matched;
  }

  let tz = moment(`${matched[0]} +00:00`, 'YYYY/MM/DD/HH ZZ');
  matched[1] = tz.format('YYYY');
  matched[2] = tz.format('MM');
  matched[3] = tz.format('DD');
  matched[4] = tz.format('HH');
  return matched;
};

(async () => {
  try {
    const dbDef = require('./sampledatabase.json');
    const admin = new AthenaAdmin(dbDef);
    await admin.replaceObjects(false, utcToTZ);
    await admin.migrate();
    await admin.partition();
  } catch (e) {
    console.log(e);
  }
})();
