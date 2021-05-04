create function fn_load_s3_file(aws_file character varying) returns void
    language plpgsql
as
$$
declare
	catch_text character varying;
	e1 text;
  	e2 text;
	e3 text;
	e4 text;
	sql_error character varying[];

	begin
		catch_text :=( SELECT  aws_s3.table_import_from_s3(
	    'employee_staging',
		'id, first_name, last_name, salary, department',
		'Delimiter '','' CSV HEADER',
		'employees-test-bucket',
		aws_file,
		'us-east-1',
		'[AWS_ACCESSKEY]',
		'[AWS_SECRETACCESSKEY]',
		'') );
	exception when others then
        get stacked diagnostics	e1 = pg_exception_context,
            				    e2 = returned_sqlstate,
                                e3 = pg_exception_detail,
                                e4 = message_text;

        sql_error := ARRAY[e1, e2, e3, e4];
		raise notice 'sql error %', sql_error;
		insert into load_error_log(aws_key,error_message,inserted_at,sql_function)
		values(aws_file,sql_error,now(),'fn_load_s3_file');
--		call eos_load_error(aws_file,sql_error);
--commit;
end;
$$;

alter function fn_load_s3_file(varchar) owner to postgres;

