/*
* Copyright (C) 2015 Dato, Inc.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as
* published by the Free Software Foundation, either version 3 of the
* License, or (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#ifndef FAULT_MESSAGE_FLAGS_HPP
#define FAULT_MESSAGE_FLAGS_HPP

// If this is a query. Mutually exclusive with QO_MESSAGE_FLAG_UPDATE
#define QO_MESSAGE_FLAG_QUERY  1
// If this is an update. Mutually exclusive with QO_MESSAGE_FLAG_QUERY
#define QO_MESSAGE_FLAG_UPDATE 2
// if a reply is expected.
#define QO_MESSAGE_FLAG_NOREPLY 4              // overrideable

// if the message can be sent to any master/slave
#define QO_MESSAGE_FLAG_ANY_TARGET   8



// If this bit is set, the message
// should reply with the complete serialized contents
// of the object.
#define QO_MESSAGE_FLAG_GET_SERIALIZED_CONTENTS 16

#endif
