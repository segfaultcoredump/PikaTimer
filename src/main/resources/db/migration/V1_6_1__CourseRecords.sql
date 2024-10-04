/* 
 * Copyright (C) 2019 John Garner <segfaultcoredump@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * Author:  John Garner <segfaultcoredump@gmail.com>
 * Created: Apr 13, 2019
 */

create table course_records (
    id int,
    race_id int,
    seg_id int,
    cr_time bigint,
    cr_category varchar,
    cr_sex varchar,
    cr_age varchar,
    cr_name varchar,
    cr_note varchar,
    cr_year varchar,
    cr_city varchar,
    cr_state varchar,
    cr_country varchar
    
);