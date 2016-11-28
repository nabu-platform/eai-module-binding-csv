package nabu.data.csv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.List;

import javax.jws.WebParam;
import javax.jws.WebResult;
import javax.jws.WebService;
import javax.validation.constraints.NotNull;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.libs.datastore.DatastoreOutputStream;
import be.nabu.libs.datastore.api.ContextualWritableDatastore;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.types.ComplexContentWrapperFactory;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.csv.CSVBinding;

@WebService
public class Services {
	
	private ExecutionContext context;
	private ServiceRuntime runtime;
	
	@WebResult(name = "unmarshalled")
	public Object unmarshal(@WebParam(name = "input") @NotNull InputStream input, @NotNull @WebParam(name = "type") String type, @WebParam(name = "charset") Charset charset, @WebParam(name = "recordSeparator") String recordSeparator, @WebParam(name = "fieldSeparator") String fieldSeparator, @WebParam(name = "quoteCharacter") String quoteCharacter, @WebParam(name = "useHeaders") Boolean useHeaders, @WebParam(name = "windows") List<Window> windows) throws IOException, ParseException {
		ComplexType resolve = (ComplexType) EAIResourceRepository.getInstance().resolve(type);
		CSVBinding binding = new CSVBinding(resolve, charset == null ? Charset.defaultCharset() : charset);
		if (fieldSeparator != null) {
			binding.setFieldSeparator(fieldSeparator);
		}
		if (recordSeparator != null) {
			binding.setRecordSeparator(recordSeparator);
		}
		if (useHeaders != null) {
			binding.setUseHeader(useHeaders);
		}
		if (quoteCharacter != null) {
			binding.setQuoteCharacter(quoteCharacter.charAt(0));
		}
		return binding.unmarshal(input, windows == null || windows.isEmpty() ? new Window[0] : windows.toArray(new Window[windows.size()]));
	}
	
	@SuppressWarnings("unchecked")
	@WebResult(name = "marshalled")
	public InputStream marshal(@WebParam(name = "data") @NotNull Object data, @WebParam(name = "charset") Charset charset, @WebParam(name = "recordSeparator") String recordSeparator, @WebParam(name = "fieldSeparator") String fieldSeparator, @WebParam(name = "quoteCharacter") String quoteCharacter, @WebParam(name = "useHeaders") Boolean useHeaders) throws IOException {
		ComplexContent complexContent = data instanceof ComplexContent ? (ComplexContent) data : ComplexContentWrapperFactory.getInstance().getWrapper().wrap(data);
		CSVBinding binding = new CSVBinding(complexContent.getType(), charset == null ? Charset.defaultCharset() : charset);
		if (fieldSeparator != null) {
			binding.setFieldSeparator(fieldSeparator);
		}
		if (recordSeparator != null) {
			binding.setRecordSeparator(recordSeparator);
		}
		if (useHeaders != null) {
			binding.setUseHeader(useHeaders);
		}
		if (quoteCharacter != null) {
			binding.setQuoteCharacter(quoteCharacter.charAt(0));
		}
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		binding.marshal(output, complexContent);
		return new ByteArrayInputStream(output.toByteArray());
	}
	
	@SuppressWarnings("unchecked")
	@WebResult(name = "uri")
	public URI store(@WebParam(name = "data") @NotNull Object data, @WebParam(name = "charset") Charset charset, @WebParam(name = "context") String context, @WebParam(name = "recordSeparator") String recordSeparator, @WebParam(name = "fieldSeparator") String fieldSeparator, @WebParam(name = "quoteCharacter") String quoteCharacter, @WebParam(name = "useHeaders") Boolean useHeaders) throws URISyntaxException, IOException {
		ComplexContent complexContent = data instanceof ComplexContent ? (ComplexContent) data : ComplexContentWrapperFactory.getInstance().getWrapper().wrap(data);
		CSVBinding binding = new CSVBinding(complexContent.getType(), charset == null ? Charset.defaultCharset() : charset);
		if (fieldSeparator != null) {
			binding.setFieldSeparator(fieldSeparator);
		}
		if (recordSeparator != null) {
			binding.setRecordSeparator(recordSeparator);
		}
		if (useHeaders != null) {
			binding.setUseHeader(useHeaders);
		}
		if (quoteCharacter != null) {
			binding.setQuoteCharacter(quoteCharacter.charAt(0));
		}
		DatastoreOutputStream streamable = nabu.frameworks.datastore.Services.streamable(runtime, context, complexContent.getType().getName() + ".json", "application/json");
		if (streamable != null) {
			try {
				binding.marshal(streamable, data instanceof ComplexContent ? (ComplexContent) data : ComplexContentWrapperFactory.getInstance().getWrapper().wrap(data));
			}
			finally {
				streamable.close();
			}
			return streamable.getURI();
		}
		else {
			InputStream marshal = marshal(data, charset, recordSeparator, fieldSeparator, quoteCharacter, useHeaders);
			ContextualWritableDatastore<String> datastore = nabu.frameworks.datastore.Services.getAsDatastore(this.context);
			return datastore.store(context, marshal, complexContent.getType().getName() + ".json", "application/json");
		}
	}
}