const mysql = require('mysql2');
const { Client } = require('pg');

const mysqlConnection = mysql.createConnection({
    host: 'xxxxx',
    user: 'xxxxx',
    password: 'xxxxx',
    database: 'xxxxx'
});

const pgClient = new Client({
    host: 'xxxxx',
    user: 'xxx',
    password: 'xxxx',
    database: 'xxx',
    port: 5432
});

mysqlConnection.connect();
pgClient.connect();

async function migrateData() {
    try {
        // Migrating car documents table data
        let [carDocsRows] = await mysqlConnection.promise().query('SELECT * FROM car_docs');        
        for (let carDoc of carDocsRows) {
            let [docTypeRow] = await mysqlConnection.promise().query(
                'SELECT dc_type_title FROM doc_types WHERE dc_type_id = ?',
                [carDoc.car_doc_type]
            );
            let documentType = docTypeRow && docTypeRow[0] ? docTypeRow[0].dc_type_title : null;
            let columnMap = {
                id: carDoc.car_doc_id || null,
                vehicleId: carDoc.car_id || null,
                createdAt: carDoc.created_at ? carDoc.created_at : null,
                createdBy: '',
                updatedAt: '',
                updatedBy: carDoc.updated_at ? carDoc.updated_at : null,
                deleted: false,
                IPAddress: '',
                HOST: '',
                documentType: documentType,
                issuer: '',
                registrationDate: null,
                documentSrNo: null,
                expiryDate: carDoc.car_doc_expiry || null,
                url: carDoc.car_doc || null,
                isPublic: true,
                amount: 0,
                remark: null,
                status: carDoc.log_status || null,
            };

            let columns = Object.keys(columnMap).filter((key) => columnMap[key] !== null && columnMap[key] !== '');
            let values = columns.map((key) => columnMap[key]);

            let placeholders = values.map((_, index) => `$${index + 1}`).join(', ');

            let query = `
        INSERT INTO "vehicleDocuments" ("${columns.join('", "')}")
        VALUES (${placeholders})
        ON CONFLICT (id) DO NOTHING
      `;
            await pgClient.query(query, values);
        }

        console.log('Car documents migration completed successfully');

        // Migrating vehicles table data
        let [vehiclesRows] = await mysqlConnection.promise().query('SELECT * FROM vehicles');

        for (let vehicle of vehiclesRows) {
            let [modelRow] = await mysqlConnection.promise().query(
                'SELECT model_name FROM car_models WHERE model_id = ?',
                [vehicle.vmodel]
            );
            let model = modelRow && modelRow[0] ? modelRow[0].model_name : null;

            let [brandRow] = await mysqlConnection.promise().query(
                'SELECT make_name FROM car_makes WHERE make_id = ?',
                [vehicle.vmake]
            );
            let brand = brandRow && brandRow[0] ? brandRow[0].make_name : null;

            let columnMap = {
                id: vehicle.vid || null,
                vendorId: vehicle.vowner || null,
                brand: brand,
                model: model,
                registrationNumberPlate: vehicle.vname || null,
                yearOfManufacturing: vehicle.vmake_yr || null,
                fuelType: vehicle.vfuel_type || null,
                seatingCapacity: 5,
                airBags: '',
                milage: vehicle.vtotal_km || null,
                securityAmount: vehicle.vdeposite || null,
                registrationDate: vehicle.reg_date || null,
                dailyRentalPrice: 0,
                color: '',
                engineNumber: vehicle.eng_no || null,
                chasisNumber: vehicle.chasis_number || null,
                kmReading: vehicle.km_reading || null,
                isAirConditional: vehicle.ac_type || null,
                createdAt: '',
                createdBy: '',
                updatedAt: '',
                updatedBy: '',
                deleted: false,
                IPAddress: '',
                HOST: '',
                commissionType: '',
                commissionValue: 0,
                remark: '',
                status: vehicle.v_status || null,
                vehicleServiceIntervalKm: 0,
                currentFuel: vehicle.vfuel_lvl || null,
                rentalVid: vehicle.vid2 || null,
                fast_tag_id: vehicle.fast_tag_id || null,
                gallery_imgs: vehicle.gallery_imgs || null,
                max_fuel_lv: vehicle.max_fuel_lv || null,
            };

            let columns = Object.keys(columnMap).filter((key) => columnMap[key] !== null && columnMap[key] !== '');
            let values = columns.map((key) => columnMap[key]);

            let placeholders = values.map((_, index) => `$${index + 1}`).join(', ');

            let query = `
    INSERT INTO "vehicles" ("${columns.join('", "')}")
    VALUES (${placeholders})
    ON CONFLICT (id) DO NOTHING
    `;
            await pgClient.query(query, values);
        }

        console.log('Vehicles migration completed successfully');

        // Migrating vendor table data
        const [vendorsRows] = await mysqlConnection.promise().query('SELECT * FROM vendors');

        for (const vendor of vendorsRows) {
            let columnMap = {
                id: vendor.vendor_id || null,
                bankName: '',
                ifscCode: '',
                accountNumber: '',
                accountName: '',
                gstNumber: vendor.vendor_gst || null,
                companyName: vendor.vendor_name || null,
                contactNumber: vendor.vendor_phone || null,
                ownerName: vendor.vendor_name || null,
                address: vendor.corporate_address || null,
                pincode: '',
                createdAt: vendor.vendor_regdate || null,
                createdBy: '',
                updatedAt: '',
                updatedBy: '',
                deleted: false,
                IPAddress: '',
                HOST: '',
                commissionType: '',
                commissionValue: 0,
                remark: ''
            };

            let columns = Object.keys(columnMap).filter((key) => columnMap[key] !== null && columnMap[key] !== '');
            let values = columns.map((key) => columnMap[key]);

            let placeholders = values.map((_, index) => `$${index + 1}`).join(', ');

            let query = `
        INSERT INTO "vendors" ("${columns.join('", "')}")
        VALUES (${placeholders})
        ON CONFLICT (id) DO NOTHING
      `;
            await pgClient.query(query, values);
        }

        console.log('Vendors migration completed successfully');

    } catch (error) {
        console.error('Error migrating data:', error);
    } finally {
        mysqlConnection.end();
        pgClient.end();
    }
}

migrateData();
