/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.eai.module.binding.csv;

import java.nio.charset.Charset;
import java.util.List;

import be.nabu.eai.module.rest.api.BindingProvider;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.binding.api.MarshallableBinding;
import be.nabu.libs.types.binding.api.UnmarshallableBinding;
import be.nabu.libs.types.binding.csv.CSVBinding;
import be.nabu.utils.mime.api.Header;
import be.nabu.utils.mime.impl.MimeUtils;

public class CSVBindingProvider implements BindingProvider {

	@Override
	public UnmarshallableBinding getUnmarshallableBinding(ComplexType type, Charset charset, Header... headers) {
		String contentType = MimeUtils.getContentType(headers);
		if ("text/csv".equals(contentType)) {
			return getBinding(type, charset, headers);
		}
		return null;
	}

	@Override
	public MarshallableBinding getMarshallableBinding(ComplexType type, Charset charset, Header... headers) {
		List<String> acceptedContentTypes = MimeUtils.getAcceptedContentTypes(headers);
		if (acceptedContentTypes != null && acceptedContentTypes.contains("text/csv")) {
			return getBinding(type, charset, headers);
		}
		return null;
	}
	
	private CSVBinding getBinding(ComplexType type, Charset charset, Header... headers) {
		CSVBinding csvBinding = new CSVBinding(type, charset);
		Header header = MimeUtils.getHeader("X-CSV-Field-Separator", headers);
		// if we have an empty header value the value was likely ";"
		String providedSeparator = header == null ? null : MimeUtils.getFullHeaderValue(header);
		if (providedSeparator != null && providedSeparator.isEmpty()) {
			providedSeparator = ";";
		}
		csvBinding.setFieldSeparator(header == null ? "," : providedSeparator);
		header = MimeUtils.getHeader("X-CSV-Use-Headers", headers);
		csvBinding.setUseHeader(header == null || header.getValue().equals("true"));
		return csvBinding;
	}

	@Override
	public String getContentType(MarshallableBinding binding) {
		return binding instanceof CSVBinding ? "text/csv" : null;
	}


}
